import { useState, useEffect } from "react";

const API = "http://127.0.0.1:8000";
const CHANNELS = ["email", "sms", "push", "whatsapp"];
const DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const FALLBACK_EVENT_TYPES = [
  "PAYMENT_SUCCESS","OTP_LOGIN","EXAM_ALERT",
  "ASSIGNMENT_DUE","FEE_REMINDER","PROMO","SYSTEM_ALERT",
];

const css = {
  wrap: {
    minHeight: "100vh", background: "#f5f4f0",
    fontFamily: "'Instrument Sans', 'DM Sans', sans-serif",
    display: "flex", alignItems: "flex-start", justifyContent: "center",
    padding: "48px 16px 80px",
  },
  card: {
    background: "#fff", border: "1px solid #e2e0db",
    borderRadius: 20, width: "100%", maxWidth: 580, padding: "36px 36px 28px",
    boxShadow: "0 2px 24px rgba(0,0,0,.06)",
  },
  sectionLabel: {
    fontSize: 10.5, fontWeight: 700, letterSpacing: ".1em",
    textTransform: "uppercase", color: "#a09e99", marginBottom: 14,
  },
  field: { marginBottom: 16 },
  label: { display: "block", fontSize: 13, fontWeight: 600, color: "#1a1917", marginBottom: 6 },
  input: {
    width: "100%", padding: "9px 13px", border: "1px solid #e2e0db",
    borderRadius: 9, fontSize: 14, color: "#1a1917", background: "#fff",
    outline: "none", fontFamily: "inherit", transition: "border-color .15s",
    boxSizing: "border-box",
  },
  phoneWrap: { display: "flex", gap: 8 },
  phonePrefix: {
    padding: "9px 13px", border: "1px solid #e2e0db", borderRadius: 9,
    fontSize: 14, color: "#1a1917", background: "#f5f4f0",
    fontWeight: 600, whiteSpace: "nowrap", userSelect: "none",
    display: "flex", alignItems: "center",
  },
  phoneInput: {
    flex: 1, padding: "9px 13px", border: "1px solid #e2e0db",
    borderRadius: 9, fontSize: 14, color: "#1a1917", background: "#fff",
    outline: "none", fontFamily: "inherit", boxSizing: "border-box",
  },
  section: { marginBottom: 32 },
  divider: { border: "none", borderTop: "1px solid #e8e6e1", margin: "24px 0" },
};

function Pill({ label, active, onClick, color = "#1a1917" }) {
  return (
    <span onClick={onClick} style={{
      padding: "4px 12px", borderRadius: 99, fontSize: 12, fontWeight: 600,
      cursor: "pointer", userSelect: "none", transition: "all .12s",
      border: active ? `1.5px solid ${color}` : "1.5px solid #e2e0db",
      background: active ? color : "#fafaf9",
      color: active ? "#fff" : "#9e9c99",
    }}>{label}</span>
  );
}

function DayPill({ day, active, onClick }) {
  return (
    <span onClick={onClick} style={{
      width: 36, height: 30, borderRadius: 7, display: "inline-flex",
      alignItems: "center", justifyContent: "center",
      fontSize: 11, fontWeight: 700, cursor: "pointer", userSelect: "none",
      transition: "all .12s",
      border: active ? "1.5px solid #1a1917" : "1.5px solid #e2e0db",
      background: active ? "#1a1917" : "#fff",
      color: active ? "#fff" : "#b0aea9",
    }}>{day}</span>
  );
}

function RemoveBtn({ onClick }) {
  const [hover, setHover] = useState(false);
  return (
    <button onClick={onClick}
      onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)}
      style={{
        width: 28, height: 28, borderRadius: 7, border: "1px solid",
        borderColor: hover ? "#fca5a5" : "#e2e0db",
        background: hover ? "#fef2f2" : "#fff",
        color: hover ? "#ef4444" : "#b0aea9",
        cursor: "pointer", fontSize: 17, display: "flex",
        alignItems: "center", justifyContent: "center", flexShrink: 0,
        transition: "all .12s",
      }}>×</button>
  );
}

function AddBtn({ onClick, label }) {
  return (
    <button onClick={onClick}
      style={{
        width: "100%", padding: "8px 14px", marginTop: 6,
        border: "1.5px dashed #d4d2cc", borderRadius: 9,
        background: "none", color: "#6b6965", fontSize: 13, fontWeight: 600,
        cursor: "pointer", fontFamily: "inherit", transition: "all .12s",
      }}
      onMouseEnter={e => { e.currentTarget.style.background="#f5f4f0"; e.currentTarget.style.color="#1a1917"; }}
      onMouseLeave={e => { e.currentTarget.style.background="none"; e.currentTarget.style.color="#6b6965"; }}
    >{label}</button>
  );
}

function DndRow({ data, onChange, onRemove }) {
  const toggleDay = (day) => {
    const days = data.days.includes(day) ? data.days.filter(d => d !== day) : [...data.days, day];
    onChange({ ...data, days });
  };
  return (
    <div style={{ background:"#fafaf9", border:"1px solid #e8e6e1", borderRadius:12, padding:16, marginBottom:10 }}>
      <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:12 }}>
        <span style={{ fontSize:13, fontWeight:600, color:"#1a1917" }}>DND window</span>
        <RemoveBtn onClick={onRemove} />
      </div>
      <div style={{ display:"flex", gap:5, flexWrap:"wrap", marginBottom:12 }}>
        {DAYS.map(d => <DayPill key={d} day={d} active={data.days.includes(d)} onClick={() => toggleDay(d)} />)}
      </div>
      <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:10 }}>
        <div>
          <label style={css.label}>Start</label>
          <input type="time" value={data.start} onChange={e => onChange({...data, start:e.target.value})} style={css.input} />
        </div>
        <div>
          <label style={css.label}>End</label>
          <input type="time" value={data.end} onChange={e => onChange({...data, end:e.target.value})} style={css.input} />
        </div>
      </div>
    </div>
  );
}

function EventPrefRow({ data, eventTypes, onChange, onRemove }) {
  const toggleChannel = (ch) => {
    const channels = data.channels.includes(ch)
      ? data.channels.filter(c => c !== ch)
      : [...data.channels, ch];
    onChange({ ...data, channels });
  };
  const togglePerEventDnd = () => {
    onChange({ ...data, dnd_mode: data.dnd_mode === "per_event" ? "none" : "per_event", dnd_windows: data.dnd_windows || [] });
  };
  return (
    <div style={{ border:"1px solid #e8e6e1", borderRadius:12, padding:16, marginBottom:10, background:"#fff" }}>
      <div style={{ display:"flex", gap:10, alignItems:"center", marginBottom:12 }}>
        <select value={data.event_type} onChange={e => onChange({...data, event_type:e.target.value})}
          style={{ flex:1, padding:"8px 11px", border:"1px solid #e2e0db", borderRadius:9,
            fontSize:13, color:"#1a1917", fontFamily:"inherit", background:"#fff", cursor:"pointer" }}>
          {eventTypes.map(et => <option key={et} value={et}>{et}</option>)}
        </select>
        <RemoveBtn onClick={onRemove} />
      </div>
      <div style={{ marginBottom:12 }}>
        <span style={{ fontSize:11, fontWeight:700, color:"#a09e99", letterSpacing:".07em", textTransform:"uppercase" }}>
          Channels
        </span>
        <div style={{ display:"flex", gap:6, flexWrap:"wrap", marginTop:8 }}>
          {CHANNELS.map(ch => (
            <Pill key={ch} label={ch} active={data.channels.includes(ch)} onClick={() => toggleChannel(ch)} color="#1a1917" />
          ))}
        </div>
      </div>
      <div onClick={togglePerEventDnd} style={{
        display:"inline-flex", alignItems:"center", gap:8, cursor:"pointer", fontSize:12, fontWeight:600,
        color: data.dnd_mode === "per_event" ? "#1a1917" : "#a09e99",
        padding:"5px 10px", borderRadius:7,
        border: `1.5px solid ${data.dnd_mode === "per_event" ? "#1a1917" : "#e2e0db"}`,
        background: data.dnd_mode === "per_event" ? "#f5f4f0" : "#fff",
        transition:"all .12s", userSelect:"none",
      }}>
        <span style={{
          width:14, height:14, borderRadius:3,
          border:`2px solid ${data.dnd_mode === "per_event" ? "#1a1917" : "#d4d2cc"}`,
          background: data.dnd_mode === "per_event" ? "#1a1917" : "#fff",
          display:"inline-flex", alignItems:"center", justifyContent:"center", flexShrink:0,
        }}>
          {data.dnd_mode === "per_event" && <span style={{color:"#fff", fontSize:10, lineHeight:1}}>✓</span>}
        </span>
        Custom DND for this event
      </div>
      {data.dnd_mode === "per_event" && (
        <div style={{ marginTop:12 }}>
          {(data.dnd_windows || []).map((dw, i) => (
            <DndRow key={i} data={dw}
              onChange={val => { const w = [...(data.dnd_windows||[])]; w[i]=val; onChange({...data, dnd_windows:w}); }}
              onRemove={() => onChange({...data, dnd_windows:(data.dnd_windows||[]).filter((_,idx)=>idx!==i)})} />
          ))}
          <AddBtn
            onClick={() => onChange({...data, dnd_windows:[...(data.dnd_windows||[]), {days:[],start:"",end:"",tz:"Asia/Kolkata"}]})}
            label="+ Add DND window for this event" />
        </div>
      )}
    </div>
  );
}

function Toast({ msg, type }) {
  if (!msg) return null;
  return (
    <div style={{
      marginTop:14, padding:"10px 16px", borderRadius:9, fontSize:13, fontWeight:600,
      background: type === "ok" ? "#f0fdf4" : "#fef2f2",
      color:      type === "ok" ? "#16a34a" : "#ef4444",
      border: `1px solid ${type === "ok" ? "#bbf7d0" : "#fecaca"}`,
    }}>{msg}</div>
  );
}

// ── MAIN COMPONENT ────────────────────────────────────────────────────────────
export default function UserPreferences() {
  const [email, setEmail]           = useState("");
  const [phoneNum, setPhoneNum]     = useState("");
  const [waNum, setWaNum]           = useState("");
  const [eventPrefs, setEventPrefs] = useState([]);
  const [globalDnds, setGlobalDnds] = useState([]);
  const [dndMode, setDndMode]       = useState("global");
  const [eventTypes, setEventTypes] = useState(FALLBACK_EVENT_TYPES);
  const [userId, setUserId]         = useState("");
  const [toast, setToast]           = useState({ msg:"", type:"" });
  const [loading, setLoading]       = useState(false);

  // fetch event types from this user's notification history
  const fetchEventTypes = async (uid) => {
    if (!uid.trim()) return;
    try {
      const res = await fetch(`${API}/user-event-types/${uid.trim()}`);
      if (res.ok) {
        const data = await res.json();
        if (data.event_types?.length) {
          // merge fetched types with fallback so new ones appear instantly
          setEventTypes(prev => {
            const merged = [...new Set([...data.event_types, ...prev])];
            return merged;
          });
        }
      }
    } catch {}
  };

  const showToast = (msg, type) => {
    setToast({ msg, type });
    setTimeout(() => setToast({ msg:"", type:"" }), 5000);
  };

  const save = async () => {
    setLoading(true);

    const event_channel_preferences = eventPrefs.map(ep => ({
      event_type:  ep.event_type,
      channels:    ep.channels,
      dnd_mode:    dndMode === "per_event" ? ep.dnd_mode : "none",
      dnd_windows: dndMode === "per_event" && ep.dnd_mode === "per_event" ? ep.dnd_windows : [],
    }));

    const payload = {
      email:     email || null,
      phone:     phoneNum ? `+91${phoneNum}` : null,
      wa_number: waNum   ? `+91${waNum}`    : null,
      event_channel_preferences,
      dnd_windows: dndMode === "global" ? globalDnds : [],
    };

    try {
      const res = await fetch(`${API}/preferences`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: "Something went wrong" }));
        showToast("Error: " + (err.detail || "Something went wrong"), "err");
        return;
      }
      showToast("Preferences saved successfully.", "ok");
    } catch (e) {
      showToast("Network error: " + e.message, "err");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={css.wrap}>
      <div style={css.card}>

        <div style={{ marginBottom:32 }}>
          <h1 style={{ fontSize:22, fontWeight:700, color:"#1a1917", marginBottom:4 }}>
            Notification preferences
          </h1>
          <p style={{ fontSize:13, color:"#9e9c99" }}>
            Choose channels per event type and configure do-not-disturb windows.
          </p>
        </div>

        {/* ── CONTACT INFO ── */}
        <div style={css.section}>
          <div style={css.sectionLabel}>Contact info</div>

          <div style={css.field}>
            <label style={css.label}>User ID</label>
            <input
              value={userId}
              onChange={e => setUserId(e.target.value)}
              onBlur={e => fetchEventTypes(e.target.value)}
              placeholder="e.g. mahi_09"
              style={css.input}
            />
            <span style={{ fontSize:11, color:"#b0aea9", marginTop:4, display:"block" }}>
              Tab out to load your event types from notification history
            </span>
          </div>

          <div style={css.field}>
            <label style={css.label}>Email</label>
            <input value={email} onChange={e => setEmail(e.target.value)}
              placeholder="you@example.com" style={css.input} />
          </div>

          <div style={css.field}>
            <label style={css.label}>Phone</label>
            <div style={css.phoneWrap}>
              <div style={css.phonePrefix}>🇮🇳 +91</div>
              <input value={phoneNum} onChange={e => setPhoneNum(e.target.value.replace(/\D/g,""))}
                placeholder="9876543210" maxLength={10} style={css.phoneInput} />
            </div>
          </div>

          <div style={css.field}>
            <label style={css.label}>WhatsApp</label>
            <div style={css.phoneWrap}>
              <div style={css.phonePrefix}>🇮🇳 +91</div>
              <input value={waNum} onChange={e => setWaNum(e.target.value.replace(/\D/g,""))}
                placeholder="9876543210" maxLength={10} style={css.phoneInput} />
            </div>
          </div>
        </div>

        <hr style={css.divider} />

        {/* ── EVENT CHANNEL PREFERENCES ── */}
        <div style={css.section}>
          <div style={css.sectionLabel}>Event channel preferences</div>
          <p style={{ fontSize:12, color:"#a09e99", marginBottom:14 }}>
            Select which channels you want for each event type.
          </p>
          {eventPrefs.map((ep, i) => (
            <EventPrefRow key={i} data={ep} eventTypes={eventTypes}
              onChange={val => setEventPrefs(prev => prev.map((e,idx) => idx===i ? val : e))}
              onRemove={() => setEventPrefs(prev => prev.filter((_,idx) => idx!==i))} />
          ))}
          <AddBtn
            onClick={() => setEventPrefs(prev => [
              ...prev,
              { event_type: eventTypes[0]||"PAYMENT_SUCCESS", channels:[], dnd_mode:"none", dnd_windows:[] }
            ])}
            label="+ Add event type preference" />
        </div>

        <hr style={css.divider} />

        {/* ── DND MODE ── */}
        <div style={css.section}>
          <div style={css.sectionLabel}>Do-not-disturb</div>
          <p style={{ fontSize:12, color:"#a09e99", marginBottom:14 }}>
            Apply one DND schedule to all events, or configure per event type above.
          </p>
          <div style={{ display:"flex", gap:8, marginBottom:20 }}>
            {[
              { val:"global",    label:"Global (all events)" },
              { val:"per_event", label:"Per event type" },
            ].map(opt => (
              <button key={opt.val} onClick={() => setDndMode(opt.val)} style={{
                padding:"8px 16px", borderRadius:9, fontSize:13, fontWeight:600,
                cursor:"pointer", fontFamily:"inherit", transition:"all .12s",
                border: dndMode === opt.val ? "1.5px solid #1a1917" : "1.5px solid #e2e0db",
                background: dndMode === opt.val ? "#1a1917" : "#fff",
                color: dndMode === opt.val ? "#fff" : "#6b6965",
              }}>{opt.label}</button>
            ))}
          </div>
          {dndMode === "global" && (
            <>
              {globalDnds.map((dw, i) => (
                <DndRow key={i} data={dw}
                  onChange={val => setGlobalDnds(prev => prev.map((d,idx) => idx===i ? val : d))}
                  onRemove={() => setGlobalDnds(prev => prev.filter((_,idx) => idx!==i))} />
              ))}
              <AddBtn
                onClick={() => setGlobalDnds(prev => [...prev, { days:[], start:"", end:"", tz:"Asia/Kolkata" }])}
                label="+ Add DND window" />
            </>
          )}
          {dndMode === "per_event" && (
            <div style={{ padding:"12px 16px", background:"#fafaf9", border:"1px solid #e8e6e1",
              borderRadius:10, fontSize:13, color:"#6b6965" }}>
              Enable "Custom DND for this event" inside each event preference above.
            </div>
          )}
        </div>

        <hr style={css.divider} />

        <button onClick={save} disabled={loading} style={{
          width:"100%", padding:"13px", borderRadius:11, border:"none",
          background: loading ? "#d4d2cc" : "#1a1917", color:"#fff",
          fontSize:15, fontWeight:700, cursor: loading ? "not-allowed" : "pointer",
          fontFamily:"inherit", transition:"background .15s", letterSpacing:".01em",
        }}>
          {loading ? "Saving…" : "Save preferences"}
        </button>

        <Toast msg={toast.msg} type={toast.type} />
      </div>
    </div>
  );
}