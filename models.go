package main

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Link models sent between Ship of Harkinian co-op teammates.
//
// A client that doesn't have a teammate's model asks them for it (MODEL_REQUEST) and gets it back in chunks
// (MODEL_CHUNK), both relayed with targetClientId. A model's id is a hash of its files. The server keeps every
// model that passes through it, once it checked that it matches its id, and answers later requests for it itself,
// so each model is only uploaded once.

const MODEL_MAX_SIZE = 64 * 1024 * 1024 // Same limit as the clients
const MODEL_CHUNK_SIZE = 32 * 1024
const MODEL_CHUNK_INTERVAL = 15 * time.Millisecond // About what a client sends at 60 frames a second
const MODEL_SEND_QUEUE_LIMIT = sendQueueSize / 4   // Chunks wait while the client's queue fills past this
const MODEL_UPLOAD_TIMEOUT = time.Minute
const MODEL_MAX_UPLOADS = 16 // Models being put together at once
const DEFAULT_MODEL_CACHE_MB = 2048

var modelIdPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

type modelUpload struct {
	modelId   string
	total     int
	data      []byte
	lastChunk time.Time
}

type ModelCache struct {
	dir      string
	maxBytes int64
	mu       sync.Mutex
	uploads  map[string]*modelUpload // By model id, sender and receiver
	sending  map[string]bool         // By client id and model id
}

func NewModelCache() *ModelCache {
	maxMB := int64(DEFAULT_MODEL_CACHE_MB)
	if value, ok := os.LookupEnv("MODEL_CACHE_MB"); ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			log.Printf("Invalid MODEL_CACHE_MB %q, using %d", value, DEFAULT_MODEL_CACHE_MB)
		} else {
			maxMB = parsed
		}
	}

	dir := os.Getenv("MODEL_CACHE_DIR")
	if dir == "" {
		dir = "models"
	}

	m := &ModelCache{
		dir:      dir,
		maxBytes: maxMB * 1024 * 1024,
		uploads:  make(map[string]*modelUpload),
		sending:  make(map[string]bool),
	}

	if m.enabled() {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("Can't create the model cache in %s, models won't be kept: %v", dir, err)
			m.maxBytes = 0
		}
	}

	return m
}

func (m *ModelCache) enabled() bool {
	return m.maxBytes > 0
}

func (m *ModelCache) path(modelId string) string {
	return filepath.Join(m.dir, modelId+".o2r")
}

// Answers a MODEL_REQUEST from the cache. Returns false when we don't have the model, then the request goes on to
// the teammate it was meant for.
func (m *ModelCache) handleRequest(c *Client, packet string) bool {
	modelId := gjson.Get(packet, "modelId").String()
	if !m.enabled() || !modelIdPattern.MatchString(modelId) {
		return false
	}

	data, err := os.ReadFile(m.path(modelId))
	if err != nil || len(data) == 0 {
		return false
	}

	// Recently used models are the last to go when the cache is full
	now := time.Now()
	os.Chtimes(m.path(modelId), now, now)

	key := fmt.Sprintf("%d|%s", c.id, modelId)
	m.mu.Lock()
	if m.sending[key] {
		m.mu.Unlock()
		return true
	}
	m.sending[key] = true
	m.mu.Unlock()

	log.Printf("Sending model %s (%d bytes) to client %d from the cache", modelId, len(data), c.id)
	go m.send(c, modelId, data, key)
	return true
}

func (m *ModelCache) send(c *Client, modelId string, data []byte, key string) {
	defer func() {
		m.mu.Lock()
		delete(m.sending, key)
		m.mu.Unlock()
	}()

	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	for offset := 0; offset < len(data); {
		c.mu.Lock()
		connected := c.conn == conn && c.sendCh != nil
		queued := 0
		if connected {
			queued = len(c.sendCh)
		}
		c.mu.Unlock()

		if !connected {
			return
		}
		if queued > MODEL_SEND_QUEUE_LIMIT {
			time.Sleep(MODEL_CHUNK_INTERVAL)
			continue
		}

		size := min(MODEL_CHUNK_SIZE, len(data)-offset)
		packet := `{"type":"MODEL_CHUNK","quiet":true}`
		packet, _ = sjson.Set(packet, "modelId", modelId)
		packet, _ = sjson.Set(packet, "offset", offset)
		packet, _ = sjson.Set(packet, "total", len(data))
		packet, _ = sjson.Set(packet, "data", base64.StdEncoding.EncodeToString(data[offset:offset+size]))
		c.sendPacket(packet)

		offset += size
		time.Sleep(MODEL_CHUNK_INTERVAL)
	}
}

// Keeps the model a MODEL_CHUNK being relayed belongs to, once all of it went through.
func (m *ModelCache) handleChunk(c *Client, packet string) {
	modelId := gjson.Get(packet, "modelId").String()
	if !m.enabled() || !modelIdPattern.MatchString(modelId) {
		return
	}

	offset := gjson.Get(packet, "offset").Int()
	total := gjson.Get(packet, "total").Int()
	key := fmt.Sprintf("%s|%d|%d", modelId, c.id, gjson.Get(packet, "targetClientId").Uint())

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for k, upload := range m.uploads {
		if now.Sub(upload.lastChunk) > MODEL_UPLOAD_TIMEOUT {
			delete(m.uploads, k)
		}
	}

	upload, ok := m.uploads[key]
	if offset == 0 {
		if total <= 0 || total > MODEL_MAX_SIZE || total > m.maxBytes {
			return
		}
		if _, err := os.Stat(m.path(modelId)); err == nil {
			return
		}
		if !ok && len(m.uploads) >= MODEL_MAX_UPLOADS {
			return
		}
		upload = &modelUpload{modelId: modelId, total: int(total), data: make([]byte, 0, total)}
		m.uploads[key] = upload
	} else if !ok {
		return
	}

	decoded, err := base64.StdEncoding.DecodeString(gjson.Get(packet, "data").String())
	if err != nil || int(total) != upload.total || int(offset) != len(upload.data) ||
		len(upload.data)+len(decoded) > upload.total {
		delete(m.uploads, key)
		return
	}
	upload.data = append(upload.data, decoded...)
	upload.lastChunk = now

	if len(upload.data) < upload.total {
		return
	}

	delete(m.uploads, key)
	go m.store(upload)
}

func (m *ModelCache) store(upload *modelUpload) {
	id, err := hashModelArchive(upload.data)
	if err != nil || id != upload.modelId {
		log.Printf("Model %s doesn't match its id, not kept", upload.modelId)
		return
	}

	path := m.path(upload.modelId)
	tempPath := path + ".part"
	if err := os.WriteFile(tempPath, upload.data, 0644); err != nil {
		log.Printf("Can't write model %s: %v", upload.modelId, err)
		return
	}
	if err := os.Rename(tempPath, path); err != nil {
		log.Printf("Can't write model %s: %v", upload.modelId, err)
		os.Remove(tempPath)
		return
	}

	log.Printf("Kept model %s (%d bytes)", upload.modelId, len(upload.data))
	m.trim()
}

// Removes the models used the longest ago until the cache fits its size again
func (m *ModelCache) trim() {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return
	}

	type cachedModel struct {
		path    string
		size    int64
		modTime time.Time
	}
	var models []cachedModel
	var totalSize int64
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".o2r" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		models = append(models, cachedModel{filepath.Join(m.dir, entry.Name()), info.Size(), info.ModTime()})
		totalSize += info.Size()
	}

	sort.Slice(models, func(i, j int) bool { return models[i].modTime.Before(models[j].modTime) })
	for _, model := range models {
		if totalSize <= m.maxBytes {
			break
		}
		if os.Remove(model.path) == nil {
			log.Printf("Model cache full, removed %s", filepath.Base(model.path))
			totalSize -= model.size
		}
	}
}

func (m *ModelCache) stats() (count int, size int64) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return 0, 0
	}
	for _, entry := range entries {
		if info, err := entry.Info(); err == nil && !entry.IsDir() && filepath.Ext(entry.Name()) == ".o2r" {
			count++
			size += info.Size()
		}
	}
	return count, size
}

// MARK: - Model ids

// The id the clients give a model: a CRC64 over the path and contents of each of its files, in path order
// (HashModelFiles in SoH's CoopModels.cpp, with libultraship's update_crc64)
func hashModelArchive(data []byte) (string, error) {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", err
	}

	files := make(map[string][]byte)
	for _, file := range reader.File {
		if file.UncompressedSize64 > MODEL_MAX_SIZE {
			return "", fmt.Errorf("%s is too big", file.Name)
		}
		stream, err := file.Open()
		if err != nil {
			return "", err
		}
		contents, err := io.ReadAll(io.LimitReader(stream, MODEL_MAX_SIZE+1))
		stream.Close()
		if err != nil {
			return "", err
		}
		if len(contents) > MODEL_MAX_SIZE {
			return "", fmt.Errorf("%s is too big", file.Name)
		}
		files[file.Name] = contents
	}
	if len(files) == 0 {
		return "", fmt.Errorf("no files")
	}

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)

	crc := uint64(0xffffffffffffffff)
	for _, name := range names {
		crc = updateCrc64([]byte(name), crc)
		crc = updateCrc64(files[name], crc)
	}
	return fmt.Sprintf("%016x", crc), nil
}

var crc64Table = func() [256]uint64 {
	const poly = 0x42f0e1eba9ea3693 // ECMA-182, most significant bit first
	var table [256]uint64
	for i := range table {
		crc := uint64(i) << 56
		for bit := 0; bit < 8; bit++ {
			if crc&(1<<63) != 0 {
				crc = crc<<1 ^ poly
			} else {
				crc <<= 1
			}
		}
		table[i] = crc
	}
	return table
}()

func updateCrc64(data []byte, crc uint64) uint64 {
	for _, b := range data {
		crc = crc64Table[byte(crc>>56)^b] ^ (crc << 8)
	}
	return ^crc
}
