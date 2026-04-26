import { useState, useEffect } from "react";
import axios from "axios";

const API_KEY_STORAGE = "notify_x_api_key";
const API_BASE_URL = "http://localhost:8000";

const DEFAULT_JSON = `{
  "client_id": "client_001",
  "event_type": "PAYMENT_SUCCESS",
  "recipients": [
    {
      "user_id": "user_1",
      "email": "test@gmail.com",
      "phone": "9876543210",
      "wa_number": "9876543210",
      "fcm_token": "token"
    }
  ],
  "channels_requested": ["email"],
  "priority": "HIGH",
  "content": {
    "message": "Payment successful!"
  }
}`;

function validateJSON(str) {
  try {
    JSON.parse(str);
    return { valid: true, error: null };
  } catch (e) {
    return { valid: false, error: e.message };
  }
}

function getLineCount(str) {
  return str.split("\n").length;
}

function ensureAttachmentInPayload(jsonText, attachment) {
  const parsed = JSON.parse(jsonText);
  const next = { ...parsed };
  const content = next.content && typeof next.content === "object" && !Array.isArray(next.content)
    ? { ...next.content }
    : {};
  const attachments = Array.isArray(content.attachments) ? [...content.attachments] : [];

  attachments.push({
    file_id: attachment.file_id,
    name: attachment.name || undefined,
    mime_type: attachment.mime_type || undefined,
    size_bytes: attachment.size_bytes ?? undefined,
  });

  content.attachments = attachments;
  next.content = content;
  return JSON.stringify(next, null, 2);
}

export default function NotifyJSONPage() {
  const [jsonInput, setJsonInput] = useState(DEFAULT_JSON);
  const [apiKey, setApiKey] = useState("");
  const [showApiKey, setShowApiKey] = useState(false);
  const [loading, setLoading] = useState(false);
  const [toast, setToast] = useState(null);
  const [response, setResponse] = useState(null);
  const [copied, setCopied] = useState(false);
  const [uploadMode, setUploadMode] = useState("file");
  const [uploadFile, setUploadFile] = useState(null);
  const [remoteUrl, setRemoteUrl] = useState("");
  const [uploadName, setUploadName] = useState("");
  const [uploadMimeType, setUploadMimeType] = useState("");
  const [uploadSizeBytes, setUploadSizeBytes] = useState("");
  const [deliveryMode, setDeliveryMode] = useState("auto");
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState(null);
  const [attachmentInserted, setAttachmentInserted] = useState(false);
  const [isCompact, setIsCompact] = useState(() =>
    typeof window !== "undefined" ? window.innerWidth < 720 : false
  );

  useEffect(() => {
    try {
      const saved = localStorage.getItem(API_KEY_STORAGE);
      if (saved) setApiKey(saved);
    } catch {
      /* ignore */
    }
  }, []);

  useEffect(() => {
    const handleResize = () => setIsCompact(window.innerWidth < 720);
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const persistApiKey = (value) => {
    setApiKey(value);
    try {
      if (value.trim()) localStorage.setItem(API_KEY_STORAGE, value);
      else localStorage.removeItem(API_KEY_STORAGE);
    } catch {
      /* ignore */
    }
  };

  const { valid, error } = validateJSON(jsonInput);
  const lineCount = getLineCount(jsonInput);

  const showToast = (message, type) => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 4000);
  };

  const handleSubmit = async () => {
    if (!valid) {
      showToast("Fix JSON errors before sending", "error");
      return;
    }
    if (!apiKey.trim()) {
      showToast("Enter your X-API-Key (from registration or dashboard)", "error");
      return;
    }
    setLoading(true);
    setResponse(null);
    try {
      const payload = JSON.parse(jsonInput);
      const res = await axios.post(`${API_BASE_URL}/notify`, payload, {
        headers: { "X-API-Key": apiKey.trim() },
      });
      setResponse({ ok: true, data: res.data, status: res.status });
      showToast("Notification sent successfully", "success");
    } catch (err) {
      const errData = err.response?.data || { detail: err.message };
      const status = err.response?.status || 0;
      setResponse({ ok: false, data: errData, status });
      showToast("Request failed", "error");
    } finally {
      setLoading(false);
    }
  };

  const handleFormat = () => {
    if (!valid) return;
    setJsonInput(JSON.stringify(JSON.parse(jsonInput), null, 2));
  };

  const handleReset = () => {
    setJsonInput(DEFAULT_JSON);
    setResponse(null);
    setToast(null);
  };

  const handleCopy = () => {
    navigator.clipboard.writeText(jsonInput);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const resetUploadForm = () => {
    setUploadFile(null);
    setRemoteUrl("");
    setUploadName("");
    setUploadMimeType("");
    setUploadSizeBytes("");
    setDeliveryMode("auto");
  };

  const handleUpload = async () => {
    if (!apiKey.trim()) {
      showToast("Enter your X-API-Key before uploading media", "error");
      return;
    }

    if (uploadMode === "file" && !uploadFile) {
      showToast("Choose a file to upload", "error");
      return;
    }

    if (uploadMode === "url" && !remoteUrl.trim()) {
      showToast("Enter a remote URL to register", "error");
      return;
    }

    setUploading(true);
    setUploadResult(null);
    setAttachmentInserted(false);

    try {
      const formData = new FormData();
      if (uploadMode === "file") {
        formData.append("file", uploadFile);
      } else {
        formData.append("remote_url", remoteUrl.trim());
      }
      if (uploadName.trim()) formData.append("name", uploadName.trim());
      if (uploadMimeType.trim()) formData.append("mime_type", uploadMimeType.trim());
      if (uploadSizeBytes !== "" && !Number.isNaN(Number(uploadSizeBytes))) {
        formData.append("size_bytes", String(Number(uploadSizeBytes)));
      }
      if (deliveryMode.trim()) formData.append("delivery_mode", deliveryMode);

      const res = await axios.post(`${API_BASE_URL}/media/upload`, formData, {
        headers: {
          "X-API-Key": apiKey.trim(),
        },
      });

      setUploadResult(res.data);
      showToast("Media uploaded successfully", "success");
      if (uploadMode === "file") {
        setUploadFile(null);
      }
    } catch (err) {
      const errData = err.response?.data || { detail: err.message };
      setUploadResult({ error: errData });
      showToast("Media upload failed", "error");
    } finally {
      setUploading(false);
    }
  };

  const handleInsertAttachment = () => {
    if (!uploadResult?.file_id) {
      showToast("Upload a file first to get a file_id", "error");
      return;
    }

    try {
      const nextJson = ensureAttachmentInPayload(jsonInput, uploadResult);
      setJsonInput(nextJson);
      setAttachmentInserted(true);
      setTimeout(() => setAttachmentInserted(false), 2000);
      showToast("Attachment inserted into payload", "success");
    } catch {
      showToast("Current JSON is invalid, so the attachment could not be inserted", "error");
    }
  };

  return (
    <div style={styles.page}>
      {/* Header */}
      <div style={styles.header}>
        <div>
          <h1 style={styles.title}>Notify via JSON</h1>
          <p style={styles.subtitle}>Edit the payload and send directly to the API</p>
        </div>
        <div style={styles.headerBadge}>
          <span style={{ ...styles.dot, background: valid ? "#639922" : "#E24B4A" }} />
          <span style={{ color: valid ? "#3B6D11" : "#A32D2D", fontSize: 12, fontWeight: 500 }}>
            {valid ? "Valid JSON" : "Invalid JSON"}
          </span>
        </div>
      </div>

      {/* Toast */}
      {toast && (
        <div style={{ ...styles.toast, ...styles.toastVariants[toast.type] }}>
          <span style={styles.toastIcon}>{toast.type === "success" ? "✓" : "✕"}</span>
          {toast.message}
        </div>
      )}

      {/* API key — same header the /notify endpoint expects */}
      <div style={styles.card}>
        <div style={styles.apiKeyHeader}>
          <span style={styles.toolbarLabel}>X-API-Key</span>
          <span style={styles.apiKeyHint}>Sent on every request · stored only in this browser</span>
        </div>
        <div style={styles.apiKeyRow}>
          <input
            type={showApiKey ? "text" : "password"}
            value={apiKey}
            onChange={(e) => persistApiKey(e.target.value)}
            placeholder="nf_… paste the key shown once at signup"
            style={styles.apiKeyInput}
            spellCheck={false}
            autoComplete="off"
            aria-label="API key for X-API-Key header"
          />
          <button
            type="button"
            onClick={() => setShowApiKey((s) => !s)}
            style={styles.apiKeyToggle}
          >
            {showApiKey ? "Hide" : "Show"}
          </button>
        </div>
      </div>

      <div style={styles.card}>
        <div style={{
          ...styles.sectionHeader,
          ...(isCompact ? styles.sectionHeaderCompact : {}),
        }}>
          <div>
            <div style={styles.sectionTitle}>Media Upload</div>
            <div style={styles.sectionHint}>
              Upload once to get a <code style={styles.inlineCode}>file_id</code> for <code style={styles.inlineCode}>content.attachments</code>
            </div>
          </div>
          <div style={{
            ...styles.modeSwitch,
            ...(isCompact ? styles.modeSwitchCompact : {}),
          }}>
            <button
              type="button"
              onClick={() => setUploadMode("file")}
              style={{
                ...styles.modeBtn,
                ...(uploadMode === "file" ? styles.modeBtnActive : {}),
              }}
            >
              Local file
            </button>
            <button
              type="button"
              onClick={() => setUploadMode("url")}
              style={{
                ...styles.modeBtn,
                ...(uploadMode === "url" ? styles.modeBtnActive : {}),
              }}
            >
              Remote URL
            </button>
          </div>
        </div>

        <div style={{
          ...styles.uploadGrid,
          ...(isCompact ? styles.uploadGridCompact : {}),
        }}>
          <div style={styles.fieldBlockWide}>
            <label style={styles.fieldLabel}>
              {uploadMode === "file" ? "Choose file" : "Remote URL"}
            </label>
            {uploadMode === "file" ? (
              <div style={styles.filePickerWrap}>
                <label style={styles.filePickerBtn}>
                  <input
                    type="file"
                    onChange={(e) => setUploadFile(e.target.files?.[0] || null)}
                    style={styles.hiddenInput}
                  />
                  {uploadFile ? "Change file" : "Select file"}
                </label>
                <div style={styles.fileMeta}>
                  {uploadFile
                    ? `${uploadFile.name} · ${uploadFile.size.toLocaleString()} bytes`
                    : "No file selected"}
                </div>
              </div>
            ) : (
              <input
                type="url"
                value={remoteUrl}
                onChange={(e) => setRemoteUrl(e.target.value)}
                placeholder="https://example.com/file.pdf"
                style={styles.fieldInput}
              />
            )}
          </div>

          <div style={styles.fieldBlock}>
            <label style={styles.fieldLabel}>Name</label>
            <input
              type="text"
              value={uploadName}
              onChange={(e) => setUploadName(e.target.value)}
              placeholder="Optional display name"
              style={styles.fieldInput}
            />
          </div>

          <div style={styles.fieldBlock}>
            <label style={styles.fieldLabel}>MIME type</label>
            <input
              type="text"
              value={uploadMimeType}
              onChange={(e) => setUploadMimeType(e.target.value)}
              placeholder="application/pdf"
              style={styles.fieldInput}
            />
          </div>

          <div style={styles.fieldBlock}>
            <label style={styles.fieldLabel}>Size bytes</label>
            <input
              type="number"
              min="0"
              value={uploadSizeBytes}
              onChange={(e) => setUploadSizeBytes(e.target.value)}
              placeholder="Optional"
              style={styles.fieldInput}
            />
          </div>

          <div style={styles.fieldBlock}>
            <label style={styles.fieldLabel}>Delivery mode</label>
            <select
              value={deliveryMode}
              onChange={(e) => setDeliveryMode(e.target.value)}
              style={styles.fieldInput}
            >
              <option value="auto">auto</option>
              <option value="link_only">link_only</option>
              <option value="provider_media">provider_media</option>
            </select>
          </div>
        </div>

        <div style={styles.uploadActions}>
          <button
            type="button"
            onClick={handleUpload}
            disabled={uploading || !apiKey.trim()}
            style={{
              ...styles.primaryActionBtn,
              ...(uploading || !apiKey.trim() ? styles.sendBtnDisabled : {}),
            }}
          >
            {uploading ? "Uploading…" : "Upload media"}
          </button>
          <button type="button" onClick={resetUploadForm} style={styles.secondaryActionBtn}>
            Reset fields
          </button>
        </div>

        {uploadResult && (
          <div style={styles.uploadResult}>
            <div style={styles.responseHeader}>
              <div style={styles.toolbarLabel}>Upload result</div>
              <span style={{
                ...styles.statusBadge,
                ...(uploadResult.error ? styles.statusErr : styles.statusOk),
              }}>
                {uploadResult.error ? "Upload Error" : "Ready"}
              </span>
            </div>

            {uploadResult.error ? (
              <pre style={styles.responsePre}>
                {JSON.stringify(uploadResult.error, null, 2)}
              </pre>
            ) : (
              <div style={styles.uploadSummary}>
                <div style={styles.summaryRow}>
                  <span style={styles.summaryLabel}>file_id</span>
                  <code style={styles.summaryValue}>{uploadResult.file_id}</code>
                </div>
                <div style={styles.summaryRow}>
                  <span style={styles.summaryLabel}>name</span>
                  <span style={styles.summaryValue}>{uploadResult.name || "-"}</span>
                </div>
                <div style={styles.summaryRow}>
                  <span style={styles.summaryLabel}>storage</span>
                  <span style={styles.summaryValue}>{uploadResult.storage || "-"}</span>
                </div>
                <div style={styles.summaryRow}>
                  <span style={styles.summaryLabel}>delivery_mode</span>
                  <span style={styles.summaryValue}>{uploadResult.delivery_mode || "-"}</span>
                </div>
                <div style={styles.summaryRow}>
                  <span style={styles.summaryLabel}>url</span>
                  <a href={uploadResult.url} target="_blank" rel="noreferrer" style={styles.linkValue}>
                    {uploadResult.url}
                  </a>
                </div>

                <div style={styles.uploadActionsInline}>
                  <button type="button" onClick={handleInsertAttachment} style={styles.primaryActionBtn}>
                    {attachmentInserted ? "Inserted" : "Insert into JSON payload"}
                  </button>
                  <button
                    type="button"
                    onClick={() => navigator.clipboard.writeText(uploadResult.file_id)}
                    style={styles.secondaryActionBtn}
                  >
                    Copy file_id
                  </button>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Editor card */}
      <div style={styles.card}>
        {/* Toolbar */}
        <div style={styles.toolbar}>
          <div style={styles.toolbarLeft}>
            <span style={styles.toolbarLabel}>payload.json</span>
            <span style={styles.lineCount}>{lineCount} lines</span>
          </div>
          <div style={styles.toolbarRight}>
            <button onClick={handleCopy} style={styles.toolBtn}>
              {copied ? "✓ Copied" : "Copy"}
            </button>
            <button onClick={handleFormat} disabled={!valid} style={{
              ...styles.toolBtn,
              ...(!valid ? styles.toolBtnDisabled : {}),
            }}>
              Format
            </button>
            <button onClick={handleReset} style={styles.toolBtn}>
              Reset
            </button>
          </div>
        </div>

        {/* Editor area */}
        <div style={styles.editorWrap}>
          {/* Line numbers */}
          <div style={styles.lineNumbers} aria-hidden="true">
            {Array.from({ length: lineCount }, (_, i) => (
              <div key={i} style={styles.lineNum}>{i + 1}</div>
            ))}
          </div>
          <textarea
            value={jsonInput}
            onChange={(e) => setJsonInput(e.target.value)}
            style={styles.editor}
            spellCheck={false}
            autoComplete="off"
            autoCorrect="off"
          />
        </div>

        {/* Error bar */}
        {!valid && (
          <div style={styles.errorBar}>
            <span style={styles.errorIcon}>!</span>
            <span style={styles.errorText}>{error}</span>
          </div>
        )}
      </div>

      {/* Send button */}
      <button
        onClick={handleSubmit}
        disabled={loading || !valid || !apiKey.trim()}
        style={{
          ...styles.sendBtn,
          ...(!valid || loading || !apiKey.trim() ? styles.sendBtnDisabled : {}),
        }}
      >
        <svg width="15" height="15" viewBox="0 0 16 16" fill="none">
          <path d="M2 8L14 2L9 14L7.5 9L2 8Z" fill="currentColor" />
        </svg>
        {loading ? "Sending…" : "Send notification"}
      </button>

      {/* Response panel */}
      {response && (
        <div style={styles.responseCard}>
          <div style={styles.responseHeader}>
            <div style={styles.toolbarLabel}>Response</div>
            <span style={{
              ...styles.statusBadge,
              ...(response.ok ? styles.statusOk : styles.statusErr),
            }}>
              {response.status} {response.ok ? "OK" : "Error"}
            </span>
          </div>
          <pre style={styles.responsePre}>
            {JSON.stringify(response.data, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}

const styles = {
  page: {
    maxWidth: 640,
    margin: "0 auto",
    padding: "2rem 1rem",
    fontFamily: "system-ui, -apple-system, sans-serif",
    display: "flex",
    flexDirection: "column",
    gap: "1rem",
  },
  header: {
    display: "flex",
    alignItems: "flex-start",
    justifyContent: "space-between",
    marginBottom: "0.25rem",
  },
  title: { fontSize: 22, fontWeight: 500, color: "#111", margin: 0 },
  subtitle: { fontSize: 14, color: "#666", marginTop: 4, marginBottom: 0 },
  headerBadge: {
    display: "flex",
    alignItems: "center",
    gap: 6,
    padding: "5px 12px",
    border: "0.5px solid rgba(0,0,0,0.12)",
    borderRadius: 20,
    background: "#fff",
    marginTop: 4,
  },
  dot: {
    width: 7,
    height: 7,
    borderRadius: "50%",
    display: "inline-block",
  },

  toast: {
    display: "flex",
    alignItems: "center",
    gap: 10,
    padding: "11px 14px",
    borderRadius: 10,
    fontSize: 13,
    border: "0.5px solid",
  },
  toastVariants: {
    success: { background: "#EAF3DE", color: "#27500A", borderColor: "#97C459" },
    error: { background: "#FCEBEB", color: "#791F1F", borderColor: "#F09595" },
  },
  toastIcon: { fontWeight: 600, fontSize: 14 },

  card: {
    background: "#fff",
    border: "0.5px solid rgba(0,0,0,0.12)",
    borderRadius: 12,
    overflow: "hidden",
  },
  sectionHeader: {
    display: "flex",
    alignItems: "flex-start",
    justifyContent: "space-between",
    gap: 12,
    padding: "14px",
    borderBottom: "0.5px solid rgba(0,0,0,0.08)",
    background: "#fafaf9",
  },
  sectionHeaderCompact: {
    flexDirection: "column",
    alignItems: "stretch",
  },
  sectionTitle: {
    fontSize: 14,
    fontWeight: 600,
    color: "#222",
    marginBottom: 4,
  },
  sectionHint: {
    fontSize: 12,
    color: "#6b6965",
    lineHeight: 1.5,
  },
  inlineCode: {
    fontFamily: "ui-monospace, monospace",
    background: "#f2f1ee",
    padding: "1px 5px",
    borderRadius: 5,
    fontSize: 11,
  },
  modeSwitch: {
    display: "flex",
    alignItems: "center",
    gap: 6,
    flexShrink: 0,
  },
  modeSwitchCompact: {
    width: "100%",
    flexWrap: "wrap",
  },
  modeBtn: {
    padding: "7px 11px",
    fontSize: 12,
    border: "0.5px solid rgba(0,0,0,0.15)",
    borderRadius: 999,
    background: "#fff",
    color: "#555",
    cursor: "pointer",
  },
  modeBtnActive: {
    background: "#111",
    color: "#fff",
    borderColor: "#111",
  },

  apiKeyHeader: {
    display: "flex",
    flexDirection: "column",
    gap: 4,
    padding: "12px 14px 0",
    borderBottom: "0.5px solid rgba(0,0,0,0.06)",
  },
  apiKeyHint: {
    fontSize: 11,
    color: "#888",
  },
  apiKeyRow: {
    display: "flex",
    alignItems: "stretch",
    gap: 8,
    padding: "10px 14px 12px",
  },
  apiKeyInput: {
    flex: 1,
    fontSize: 13,
    fontFamily: "ui-monospace, monospace",
    padding: "10px 12px",
    border: "0.5px solid rgba(0,0,0,0.18)",
    borderRadius: 8,
    outline: "none",
    boxSizing: "border-box",
  },
  apiKeyToggle: {
    flexShrink: 0,
    padding: "0 14px",
    fontSize: 12,
    border: "0.5px solid rgba(0,0,0,0.18)",
    borderRadius: 8,
    background: "#fafaf9",
    color: "#444",
    cursor: "pointer",
  },
  uploadGrid: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr",
    gap: 12,
    padding: "14px",
  },
  uploadGridCompact: {
    gridTemplateColumns: "1fr",
  },
  fieldBlock: {
    display: "flex",
    flexDirection: "column",
    gap: 6,
  },
  fieldBlockWide: {
    display: "flex",
    flexDirection: "column",
    gap: 6,
    gridColumn: "1 / -1",
  },
  fieldLabel: {
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: ".04em",
    textTransform: "uppercase",
    color: "#8a867f",
  },
  fieldInput: {
    width: "100%",
    fontSize: 13,
    padding: "10px 12px",
    border: "0.5px solid rgba(0,0,0,0.18)",
    borderRadius: 8,
    outline: "none",
    boxSizing: "border-box",
    background: "#fff",
    color: "#111",
  },
  hiddenInput: {
    display: "none",
  },
  filePickerWrap: {
    display: "flex",
    alignItems: "center",
    gap: 10,
    flexWrap: "wrap",
  },
  filePickerBtn: {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    padding: "10px 12px",
    border: "0.5px solid rgba(0,0,0,0.18)",
    borderRadius: 8,
    background: "#fff",
    color: "#333",
    cursor: "pointer",
    fontSize: 13,
  },
  fileMeta: {
    fontSize: 12,
    color: "#666",
    wordBreak: "break-word",
  },
  uploadActions: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    padding: "0 14px 14px",
    flexWrap: "wrap",
  },
  primaryActionBtn: {
    padding: "10px 14px",
    border: "none",
    borderRadius: 8,
    background: "#111",
    color: "#fff",
    cursor: "pointer",
    fontSize: 13,
    fontWeight: 600,
  },
  secondaryActionBtn: {
    padding: "10px 14px",
    border: "0.5px solid rgba(0,0,0,0.18)",
    borderRadius: 8,
    background: "#fff",
    color: "#444",
    cursor: "pointer",
    fontSize: 13,
  },
  uploadResult: {
    borderTop: "0.5px solid rgba(0,0,0,0.08)",
    background: "#fff",
  },
  uploadSummary: {
    padding: "14px",
    display: "flex",
    flexDirection: "column",
    gap: 10,
  },
  summaryRow: {
    display: "grid",
    gridTemplateColumns: "110px 1fr",
    gap: 12,
    alignItems: "start",
  },
  summaryLabel: {
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: ".04em",
    textTransform: "uppercase",
    color: "#8a867f",
  },
  summaryValue: {
    fontSize: 13,
    color: "#222",
    wordBreak: "break-word",
    fontFamily: "ui-monospace, monospace",
  },
  linkValue: {
    fontSize: 13,
    color: "#2d5f9a",
    wordBreak: "break-all",
    textDecoration: "none",
  },
  uploadActionsInline: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    flexWrap: "wrap",
    marginTop: 6,
  },

  toolbar: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "10px 14px",
    borderBottom: "0.5px solid rgba(0,0,0,0.08)",
    background: "#fafaf9",
  },
  toolbarLeft: { display: "flex", alignItems: "center", gap: 10 },
  toolbarRight: { display: "flex", alignItems: "center", gap: 6 },
  toolbarLabel: {
    fontSize: 12,
    fontWeight: 500,
    color: "#555",
    fontFamily: "ui-monospace, monospace",
  },
  lineCount: { fontSize: 11, color: "#aaa" },

  toolBtn: {
    padding: "4px 10px",
    fontSize: 12,
    border: "0.5px solid rgba(0,0,0,0.18)",
    borderRadius: 6,
    background: "#fff",
    color: "#444",
    cursor: "pointer",
  },
  toolBtnDisabled: { opacity: 0.4, cursor: "not-allowed" },

  editorWrap: {
    display: "flex",
    minHeight: 320,
  },
  lineNumbers: {
    padding: "14px 0",
    minWidth: 42,
    background: "#fafaf9",
    borderRight: "0.5px solid rgba(0,0,0,0.07)",
    display: "flex",
    flexDirection: "column",
    userSelect: "none",
  },
  lineNum: {
    fontSize: 12,
    lineHeight: "21px",
    color: "#ccc",
    textAlign: "right",
    paddingRight: 10,
    fontFamily: "ui-monospace, monospace",
  },
  editor: {
    flex: 1,
    padding: "14px 14px",
    fontSize: 13,
    lineHeight: "21px",
    fontFamily: "ui-monospace, 'Cascadia Code', 'Fira Code', monospace",
    border: "none",
    outline: "none",
    resize: "vertical",
    background: "#fff",
    color: "#111",
    minHeight: 320,
    boxSizing: "border-box",
    whiteSpace: "pre",
    overflowWrap: "normal",
    overflowX: "auto",
  },

  errorBar: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    padding: "8px 14px",
    background: "#FCEBEB",
    borderTop: "0.5px solid #F09595",
  },
  errorIcon: {
    width: 16,
    height: 16,
    borderRadius: "50%",
    background: "#E24B4A",
    color: "#fff",
    fontSize: 11,
    fontWeight: 700,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    flexShrink: 0,
  },
  errorText: {
    fontSize: 12,
    color: "#791F1F",
    fontFamily: "ui-monospace, monospace",
  },

  sendBtn: {
    width: "100%",
    padding: "11px",
    background: "#111",
    color: "#fff",
    border: "none",
    borderRadius: 8,
    fontSize: 14,
    fontWeight: 500,
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
  },
  sendBtnDisabled: { opacity: 0.45, cursor: "not-allowed" },

  responseCard: {
    background: "#fff",
    border: "0.5px solid rgba(0,0,0,0.12)",
    borderRadius: 12,
    overflow: "hidden",
  },
  responseHeader: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "10px 14px",
    borderBottom: "0.5px solid rgba(0,0,0,0.08)",
    background: "#fafaf9",
  },
  statusBadge: {
    fontSize: 11,
    fontWeight: 500,
    padding: "3px 10px",
    borderRadius: 20,
  },
  statusOk: { background: "#EAF3DE", color: "#27500A" },
  statusErr: { background: "#FCEBEB", color: "#791F1F" },
  responsePre: {
    margin: 0,
    padding: "14px",
    fontSize: 12,
    lineHeight: 1.7,
    fontFamily: "ui-monospace, 'Cascadia Code', monospace",
    color: "#333",
    overflowX: "auto",
    background: "#fff",
  },
};
