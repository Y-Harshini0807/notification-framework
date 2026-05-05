import { useState, useEffect, useCallback } from "react";

const API = "http://localhost:8000";

const CHANNELS = ["email", "sms", "whatsapp", "push"];

const CHANNEL_META = {
  email:    { label: "Email",    icon: "✉", color: "#2563eb", bg: "#eff6ff", border: "#bfdbfe" },
  sms:      { label: "SMS",      icon: "💬", color: "#16a34a", bg: "#f0fdf4", border: "#bbf7d0" },
  whatsapp: { label: "WhatsApp", icon: "📱", color: "#15803d", bg: "#f0fdf4", border: "#86efac" },
  push:     { label: "Push",     icon: "🔔", color: "#9333ea", bg: "#faf5ff", border: "#d8b4fe" },
};

// All providers known to the system, keyed by channel
const AVAILABLE_PROVIDERS = {
  email:    [
    { provider_id: "sendgrid",    provider_name: "SendGrid",    description: "Cloud email delivery" },
    { provider_id: "smtp",        provider_name: "SMTP",        description: "Direct mail server" },
  ],
  sms:      [
    { provider_id: "twilio",      provider_name: "Twilio",      description: "Programmable SMS" },
    { provider_id: "sendfire",    provider_name: "SendFire",    description: "Legacy SMS gateway" },
  ],
  whatsapp: [
    { provider_id: "ultramsg",    provider_name: "UltraMsg",    description: "WhatsApp API service" },
  ],
  push:     [
    { provider_id: "firebase_fcm", provider_name: "Firebase FCM", description: "Google push messaging" },
  ],
};

/* ─── helpers ─────────────────────────────────────────────────────────────── */

function Toast({ msg, type }) {
  if (!msg) return null;
  const ok = type === "ok";
  return (
    <div style={{
      position: "fixed", bottom: 28, left: "50%", transform: "translateX(-50%)",
      background: ok ? "#1a1917" : "#7f1d1d",
      color: "#fff", padding: "10px 20px", borderRadius: 10,
      fontSize: 13, fontWeight: 600, zIndex: 999, boxShadow: "0 4px 20px rgba(0,0,0,.25)",
      display: "flex", alignItems: "center", gap: 8,
    }}>
      <span>{ok ? "✓" : "✕"}</span> {msg}
    </div>
  );
}

function Toggle({ checked, onChange, disabled }) {
  return (
    <div
      onClick={() => !disabled && onChange(!checked)}
      style={{
        width: 40, height: 22, borderRadius: 11,
        background: checked ? "#1a1917" : "#d4d2cc",
        position: "relative", cursor: disabled ? "not-allowed" : "pointer",
        transition: "background .2s", flexShrink: 0,
        opacity: disabled ? 0.5 : 1,
      }}
    >
      <div style={{
        position: "absolute", top: 3, left: checked ? 21 : 3,
        width: 16, height: 16, borderRadius: "50%", background: "#fff",
        transition: "left .2s", boxShadow: "0 1px 3px rgba(0,0,0,.2)",
      }} />
    </div>
  );
}

function NumberInput({ value, onChange, min = 1, max = 999, suffix }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <input
        type="number"
        value={value}
        min={min} max={max}
        onChange={e => onChange(Math.max(min, Math.min(max, Number(e.target.value))))}
        style={{
          width: 60, padding: "6px 8px", border: "1px solid #e2e0db",
          borderRadius: 7, fontSize: 13, color: "#1a1917", background: "#fff",
          outline: "none", fontFamily: "inherit", textAlign: "center",
        }}
      />
      {suffix && <span style={{ fontSize: 12, color: "#a09e99" }}>{suffix}</span>}
    </div>
  );
}

/* ─── ProviderCard ────────────────────────────────────────────────────────── */

function ProviderCard({ channel, provider: p, data, onChange, onSave, saving }) {
  const isActive   = data?.is_active  ?? false;
  const priority   = data?.priority   ?? 1;
  const maxRetries = data?.max_retries ?? 3;
  const timeoutMs  = data?.timeout_ms  ?? 5000;
  const isDirty    = data?._dirty ?? false;

  const chMeta = CHANNEL_META[channel];

  return (
    <div style={{
      background: "#fff", border: `1px solid ${isActive ? "#e2e0db" : "#ece9e4"}`,
      borderRadius: 14, padding: "18px 20px",
      opacity: isActive ? 1 : 0.72,
      transition: "all .15s",
      boxShadow: isActive ? "0 2px 12px rgba(0,0,0,.05)" : "none",
    }}>
      {/* Header row */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{
            width: 36, height: 36, borderRadius: 9,
            background: chMeta.bg, border: `1px solid ${chMeta.border}`,
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 16,
          }}>{chMeta.icon}</div>
          <div>
            <div style={{ fontSize: 14, fontWeight: 700, color: "#1a1917" }}>{p.provider_name}</div>
            <div style={{ fontSize: 11, color: "#a09e99", marginTop: 1 }}>{p.description}</div>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{
            fontSize: 11, fontWeight: 600, color: isActive ? "#15803d" : "#a09e99",
            background: isActive ? "#f0fdf4" : "#f5f4f0",
            border: `1px solid ${isActive ? "#bbf7d0" : "#e2e0db"}`,
            padding: "3px 9px", borderRadius: 99,
          }}>
            {isActive ? "Active" : "Inactive"}
          </span>
          <Toggle
            checked={isActive}
            onChange={v => onChange({ is_active: v, _dirty: true })}
          />
        </div>
      </div>

      {/* Config fields */}
      <div style={{
        display: "grid", gridTemplateColumns: "1fr 1fr 1fr",
        gap: 14, marginBottom: 16,
        padding: "14px 16px",
        background: "#fafaf9", border: "1px solid #ece9e4", borderRadius: 10,
      }}>
        <div>
          <div style={css.fieldLabel}>Priority</div>
          <NumberInput
            value={priority} min={1} max={10}
            onChange={v => onChange({ priority: v, _dirty: true })}
          />
          <div style={{ fontSize: 10, color: "#b0aea9", marginTop: 4 }}>Lower = first tried</div>
        </div>
        <div>
          <div style={css.fieldLabel}>Max Retries</div>
          <NumberInput
            value={maxRetries} min={0} max={10}
            onChange={v => onChange({ max_retries: v, _dirty: true })}
            suffix="times"
          />
        </div>
        <div>
          <div style={css.fieldLabel}>Timeout</div>
          <NumberInput
            value={timeoutMs} min={500} max={30000} suffix="ms"
            onChange={v => onChange({ timeout_ms: v, _dirty: true })}
          />
        </div>
      </div>

      {/* Save button */}
      <button
        onClick={onSave}
        disabled={!isDirty || saving}
        style={{
          width: "100%", padding: "9px", borderRadius: 9, border: "none",
          background: isDirty ? "#1a1917" : "#e8e6e1",
          color: isDirty ? "#fff" : "#a09e99",
          fontSize: 13, fontWeight: 600, cursor: isDirty ? "pointer" : "not-allowed",
          fontFamily: "inherit", transition: "all .15s",
        }}
      >
        {saving ? "Saving…" : isDirty ? "Save changes" : "No changes"}
      </button>
    </div>
  );
}

/* ─── ChannelSection ──────────────────────────────────────────────────────── */

function ChannelSection({ channel, providers, channelData, onChange, onSave, saving }) {
  const meta = CHANNEL_META[channel];
  const activeCount = providers.filter(p => channelData[p.provider_id]?.is_active).length;

  return (
    <div style={{ marginBottom: 36 }}>
      {/* Section header */}
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
        <div style={{
          width: 32, height: 32, borderRadius: 8,
          background: meta.bg, border: `1px solid ${meta.border}`,
          display: "flex", alignItems: "center", justifyContent: "center", fontSize: 15,
        }}>{meta.icon}</div>
        <div>
          <h2 style={{ fontSize: 16, fontWeight: 700, color: "#1a1917", margin: 0 }}>{meta.label}</h2>
        </div>
        <span style={{
          marginLeft: "auto", fontSize: 11, fontWeight: 600, color: meta.color,
          background: meta.bg, border: `1px solid ${meta.border}`,
          padding: "3px 10px", borderRadius: 99,
        }}>
          {activeCount}/{providers.length} active
        </span>
      </div>

      <div style={{
        display: "grid",
        gridTemplateColumns: providers.length > 1 ? "repeat(auto-fill, minmax(280px, 1fr))" : "1fr",
        gap: 12,
      }}>
        {providers.map(p => (
          <ProviderCard
            key={p.provider_id}
            channel={channel}
            provider={p}
            data={channelData[p.provider_id] || {}}
            onChange={patch => onChange(channel, p.provider_id, patch)}
            onSave={() => onSave(channel, p.provider_id)}
            saving={saving === `${channel}:${p.provider_id}`}
          />
        ))}
      </div>
    </div>
  );
}

/* ─── Main Page ───────────────────────────────────────────────────────────── */

export default function ProviderSettings() {
  // providerState: { [channel]: { [provider_id]: { is_active, priority, max_retries, timeout_ms, _dirty } } }
  const [providerState, setProviderState] = useState({});
  const [saving, setSaving]   = useState(null); // "channel:provider_id"
  const [loading, setLoading] = useState(true);
  const [toast, setToast]     = useState({ msg: "", type: "" });

  const showToast = (msg, type = "ok") => {
    setToast({ msg, type });
    setTimeout(() => setToast({ msg: "", type: "" }), 4000);
  };

  /* Load existing providers from API */
  const loadProviders = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API}/providers`);
      if (!res.ok) throw new Error("Failed to load");
      const { providers } = await res.json();

      // Build a state map from API data, fill missing with defaults
      const state = {};
      CHANNELS.forEach(ch => {
        state[ch] = {};
        const available = AVAILABLE_PROVIDERS[ch] || [];
        available.forEach(p => {
          const existing = providers.find(
            ap => ap.provider_id === p.provider_id && ap.channel === ch
          );
          // Defaults sourced from _PROVIDER_DEFAULTS in main.py
          const defaults = { priority: 1, max_retries: 3, timeout_ms: 5000, is_active: false };
          state[ch][p.provider_id] = { ...defaults, ...(existing || {}), _dirty: false };
        });
      });
      setProviderState(state);
    } catch (err) {
      showToast("Could not load providers from server", "err");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadProviders(); }, [loadProviders]);

  const handleChange = (channel, providerId, patch) => {
    setProviderState(prev => ({
      ...prev,
      [channel]: {
        ...prev[channel],
        [providerId]: { ...prev[channel][providerId], ...patch },
      },
    }));
  };

  const handleSave = async (channel, providerId) => {
    const key = `${channel}:${providerId}`;
    setSaving(key);
    const data = providerState[channel][providerId];
    const p = AVAILABLE_PROVIDERS[channel].find(x => x.provider_id === providerId);

    const body = {
      provider_id:   providerId,
      provider_name: p.provider_name,
      channel,
      priority:      data.priority,
      is_active:     data.is_active,
      max_retries:   data.max_retries,
      timeout_ms:    data.timeout_ms,
    };

    try {
      const res = await fetch(`${API}/providers`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: "Unknown error" }));
        throw new Error(err.detail || "Save failed");
      }
      // Clear dirty flag
      setProviderState(prev => ({
        ...prev,
        [channel]: {
          ...prev[channel],
          [providerId]: { ...prev[channel][providerId], _dirty: false },
        },
      }));
      showToast(`${p.provider_name} saved`, "ok");
    } catch (e) {
      showToast(e.message, "err");
    } finally {
      setSaving(null);
    }
  };

  /* Save all dirty providers at once */
  const handleSaveAll = async () => {
    const tasks = [];
    CHANNELS.forEach(ch => {
      Object.keys(providerState[ch] || {}).forEach(pid => {
        if (providerState[ch][pid]?._dirty) tasks.push([ch, pid]);
      });
    });
    for (const [ch, pid] of tasks) await handleSave(ch, pid);
    if (tasks.length === 0) showToast("Nothing to save", "ok");
  };

  const hasDirty = CHANNELS.some(ch =>
    Object.values(providerState[ch] || {}).some(v => v?._dirty)
  );

  if (loading) {
    return (
      <div style={{ ...css.wrap, alignItems: "center", justifyContent: "center" }}>
        <div style={{ textAlign: "center" }}>
          <div style={{ fontSize: 24, marginBottom: 12 }}>⏳</div>
          <div style={{ fontSize: 14, color: "#a09e99" }}>Loading provider configuration…</div>
        </div>
      </div>
    );
  }

  return (
    <div style={css.wrap}>
      <div style={css.inner}>

        {/* Page header */}
        <div style={{ marginBottom: 32 }}>
          <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 12 }}>
            <div>
              <h1 style={{ fontSize: 22, fontWeight: 700, color: "#1a1917", marginBottom: 4 }}>
                Provider Settings
              </h1>
              <p style={{ fontSize: 13, color: "#9e9c99", maxWidth: 480 }}>
                Enable or disable delivery providers for each notification channel.
                Adjust priority, retries, and timeout per provider.
              </p>
            </div>
            <button
              onClick={handleSaveAll}
              disabled={!hasDirty}
              style={{
                padding: "10px 20px", borderRadius: 10, border: "none",
                background: hasDirty ? "#1a1917" : "#e8e6e1",
                color: hasDirty ? "#fff" : "#b0aea9",
                fontSize: 13, fontWeight: 700, cursor: hasDirty ? "pointer" : "not-allowed",
                fontFamily: "inherit", transition: "all .15s", whiteSpace: "nowrap",
                display: "flex", alignItems: "center", gap: 6,
              }}
            >
              {hasDirty ? "💾 Save all changes" : "✓ All saved"}
            </button>
          </div>

          {/* Channel summary pills */}
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 20 }}>
            {CHANNELS.map(ch => {
              const meta = CHANNEL_META[ch];
              const providers = AVAILABLE_PROVIDERS[ch] || [];
              const active = providers.filter(p => providerState[ch]?.[p.provider_id]?.is_active).length;
              return (
                <div key={ch} style={{
                  display: "flex", alignItems: "center", gap: 6,
                  padding: "5px 12px", borderRadius: 99,
                  background: meta.bg, border: `1px solid ${meta.border}`,
                  fontSize: 12, fontWeight: 600, color: meta.color,
                }}>
                  <span>{meta.icon}</span>
                  <span>{meta.label}</span>
                  <span style={{
                    background: active > 0 ? meta.color : "#d4d2cc",
                    color: "#fff", borderRadius: 99, padding: "1px 7px", fontSize: 10,
                  }}>{active}/{providers.length}</span>
                </div>
              );
            })}
          </div>
        </div>

        <hr style={{ border: "none", borderTop: "1px solid #e8e6e1", marginBottom: 32 }} />

        {/* Channel sections */}
        {CHANNELS.map(ch => (
          <ChannelSection
            key={ch}
            channel={ch}
            providers={AVAILABLE_PROVIDERS[ch] || []}
            channelData={providerState[ch] || {}}
            onChange={handleChange}
            onSave={handleSave}
            saving={saving}
          />
        ))}

      </div>

      <Toast msg={toast.msg} type={toast.type} />
    </div>
  );
}

/* ─── Styles ──────────────────────────────────────────────────────────────── */

const css = {
  wrap: {
    minHeight: "100vh",
    background: "#f5f4f0",
    fontFamily: "'Instrument Sans', 'DM Sans', sans-serif",
    display: "flex",
    justifyContent: "center",
    padding: "48px 16px 80px",
  },
  inner: {
    width: "100%",
    maxWidth: 860,
  },
  fieldLabel: {
    fontSize: 10.5,
    fontWeight: 700,
    letterSpacing: ".09em",
    textTransform: "uppercase",
    color: "#a09e99",
    marginBottom: 8,
  },
};