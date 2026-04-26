import 'bootstrap/dist/css/bootstrap.min.css'
import {BrowserRouter, Routes, Route, Link, Navigate, useLocation} from 'react-router-dom'
import Signup from './Signup'
import Login from './Login'
import Home from './Home'
import ProviderSettings from './ProviderSettings'

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Signup />} />
        <Route path="/register" element={<Signup />} />
        <Route path="/login" element={<Login />} />
        <Route path="/home" element={<Home />} />
        <Route path="/dashboard" element={<Home />} />
        <Route path="/providers" element={<ProviderSettings />} />
        <Route path="*" element={<Navigate to="/login" replace />} />
      </Routes>
    </BrowserRouter>
  )
}

export default App
