package adapter

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/transcript/core"
)

func TestDownloadCorruptCompletedRecordShowsGapAndIncompleteState(t *testing.T) {
	for _, mode := range []string{"invalid post-metadata length", "truncated after metadata"} {
		t.Run(mode, func(t *testing.T) {
			controller, root, closeStore := adapterTestController(t, adapterTestUsers(t))
			const requestID = "corrupt-completed-record"
			handle := controller.store.Begin(core.Metadata{RequestID: requestID, UserID: "alpha", Method: http.MethodPost, URL: "/v1/responses"})
			if handle == nil {
				t.Fatal("Begin refused a small test capture")
			}
			handle.Capture("response.body", []byte("body-after-metadata"))
			handle.End(true)
			closeStore()
			if record := adapterReadRecord(t, controller, requestID); record.state != core.EndedComplete || string(record.parts["response.body"]) != "body-after-metadata" {
				t.Fatalf("record was not complete before corruption: %#v", record)
			}

			var ownedFile string
			errWalk := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.Type()&os.ModeSymlink != 0 {
					t.Fatalf("unexpected symlink in owned test root: %s", path)
				}
				if entry.IsDir() || filepath.Ext(path) != ".log" {
					return nil
				}
				info, errInfo := entry.Info()
				if errInfo != nil {
					return errInfo
				}
				if !info.Mode().IsRegular() || ownedFile != "" {
					t.Fatalf("expected one regular completed record, found %s after %s", path, ownedFile)
				}
				ownedFile = path
				return nil
			})
			if errWalk != nil || ownedFile == "" {
				t.Fatalf("locate completed record in owned root: file=%q error=%v", ownedFile, errWalk)
			}
			raw, errRead := os.ReadFile(ownedFile)
			if errRead != nil {
				t.Fatalf("read completed record: %v", errRead)
			}
			magic := []byte("CPATRANSCRIPT\x00\x01\n")
			if !bytes.HasPrefix(raw, magic) || len(raw) < len(magic)+4 {
				t.Fatalf("completed record does not use the actual magic/length-prefixed frame format: %q", raw)
			}
			metadataStart := len(magic) + 4
			metadataLength := int(binary.BigEndian.Uint32(raw[len(magic):metadataStart]))
			if metadataLength <= 0 || metadataLength > len(raw)-metadataStart-4 {
				t.Fatalf("invalid original metadata length %d for %d bytes", metadataLength, len(raw))
			}
			metadataEnd := metadataStart + metadataLength
			var metadata core.Frame
			if errDecode := json.Unmarshal(raw[metadataStart:metadataEnd], &metadata); errDecode != nil || metadata.Kind != "metadata" || metadata.Metadata == nil || metadata.Metadata.RequestID != requestID {
				t.Fatalf("first actual frame is not this request's metadata: frame=%#v error=%v", metadata, errDecode)
			}

			if mode == "invalid post-metadata length" {
				file, errOpen := os.OpenFile(ownedFile, os.O_WRONLY, 0)
				if errOpen != nil {
					t.Fatalf("open owned record for corruption: %v", errOpen)
				}
				var invalidLength [4]byte
				binary.BigEndian.PutUint32(invalidLength[:], ^uint32(0))
				n, errWrite := file.WriteAt(invalidLength[:], int64(metadataEnd))
				errClose := file.Close()
				if errWrite != nil || n != len(invalidLength) || errClose != nil {
					t.Fatalf("corrupt post-metadata length: wrote=%d write=%v close=%v", n, errWrite, errClose)
				}
			} else if errTruncate := os.Truncate(ownedFile, int64(metadataEnd)); errTruncate != nil {
				t.Fatalf("truncate owned record after metadata: %v", errTruncate)
			}

			response := renderTestDownload(t, controller, requestID)
			text := response.Body.String()
			if response.Code != http.StatusOK {
				t.Fatalf("Download status=%d body=%q, want rendered HTTP 200", response.Code, text)
			}
			if !strings.HasPrefix(response.Header().Get("Content-Type"), "text/plain") {
				t.Errorf("Download content type=%q, want native text", response.Header().Get("Content-Type"))
			}
			if got := response.Header().Get("X-Transcript-State"); got != core.EndedIncomplete {
				t.Errorf("Download header state=%q, want %q before response publication", got, core.EndedIncomplete)
			}
			for _, want := range []string{"=== REQUEST INFO ===", "Request ID: " + requestID, "[TRANSCRIPT GAP: invalid or incomplete frame]", "=== TRANSCRIPT STATUS ===", "State: " + core.EndedIncomplete} {
				if !strings.Contains(text, want) {
					t.Errorf("Download missing %q: %q", want, text)
				}
			}
			if bytes.Contains(response.Body.Bytes(), magic) || strings.Contains(text, `"Kind":`) || strings.Contains(text, `"Metadata":`) || strings.Contains(text, "State: "+core.EndedComplete) {
				t.Fatalf("Download exposed framed storage or a false complete status: %q", text)
			}
		})
	}
}
