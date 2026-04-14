import { BrowserRouter, Routes, Route, Link, Navigate } from "react-router-dom";
import NotifyPage from "./client"; 
import UserPreferences from "./UserPreferences";

export default function App() {
  return (
    <BrowserRouter>
      <div style={{ padding: 20 }}>
        {}
        <nav>
          <Link to="/">Notify</Link> |{" "}
          <Link to="/preferences">User Preferences</Link>
        </nav>

        <hr />

        <Routes>
          <Route path="/" element={<NotifyPage />} />
          <Route path="/home" element={<NotifyPage />} />
          <Route path="/preferences" element={<UserPreferences />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}