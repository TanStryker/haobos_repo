const _STORE = window.sessionStorage;

function _getItem(key) {
  try {
    return _STORE.getItem(key) || "";
  } catch (_) {
    return "";
  }
}

function _setItem(key, value) {
  try {
    _STORE.setItem(key, value);
  } catch (_) {
  }
}

function _removeItem(key) {
  try {
    _STORE.removeItem(key);
  } catch (_) {
  }
}

function _migrateLegacyAuthIfNeeded() {
  try {
    if (_getItem("token")) return;
    const legacyToken = localStorage.getItem("token") || "";
    if (!legacyToken) return;
    _setItem("token", legacyToken);
    _setItem("user_id", localStorage.getItem("user_id") || "");
    _setItem("role", localStorage.getItem("role") || "");
    localStorage.removeItem("token");
    localStorage.removeItem("user_id");
    localStorage.removeItem("role");
  } catch (_) {
  }
}

function _randId() {
  try {
    if (window.crypto && window.crypto.randomUUID) return window.crypto.randomUUID();
  } catch (_) {
  }
  return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

function _getTabId() {
  let id = _getItem("tab_id");
  if (!id) {
    id = _randId();
    _setItem("tab_id", id);
  }
  return id;
}

function getToken() {
  _migrateLegacyAuthIfNeeded();
  return _getItem("token");
}

function getRole() {
  _migrateLegacyAuthIfNeeded();
  return _getItem("role");
}

function setAuth(token, userId, role) {
  _setItem("token", token);
  _setItem("user_id", userId);
  _setItem("role", role);
}

function clearAuth() {
  _removeItem("token");
  _removeItem("user_id");
  _removeItem("role");
}

async function api(path, options = {}) {
  const headers = Object.assign({}, options.headers || {});
  headers["X-HRMS-Tab"] = _getTabId();
  const token = getToken();
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }
  if (options.body && !(options.body instanceof FormData) && !headers["Content-Type"]) {
    headers["Content-Type"] = "application/json";
  }
  const res = await fetch(path, Object.assign({}, options, { headers }));
  const contentType = res.headers.get("content-type") || "";
  let data = null;
  if (contentType.includes("application/json")) {
    data = await res.json();
  } else {
    data = await res.text();
  }
  if (!res.ok) {
    const msg = (data && data.detail) || (typeof data === "string" ? data : "请求失败");
    throw new Error(msg);
  }
  return data;
}

function qs(name) {
  const url = new URL(window.location.href);
  return url.searchParams.get(name) || "";
}

function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else node.setAttribute(k, v);
  }
  for (const c of children) {
    if (typeof c === "string") node.appendChild(document.createTextNode(c));
    else node.appendChild(c);
  }
  return node;
}

function renderNav() {
  const nav = document.getElementById("nav");
  if (!nav) return;
  nav.innerHTML = "";
  const role = getRole();
  const items = [];
  items.push({ href: "/ui/index.html", label: "首页" });
  if (role === "admin") {
    items.push({ href: "/ui/admin_employees.html", label: "员工管理" });
    items.push({ href: "/ui/admin_change_requests.html", label: "修改申请审批" });
    items.push({ href: "/ui/admin_overtime.html", label: "加班审批" });
    items.push({ href: "/ui/attendance_admin.html", label: "出勤管理" });
    items.push({ href: "/ui/salary_admin.html", label: "薪资管理" });
    items.push({ href: "/ui/system_users.html", label: "系统-账号" });
    items.push({ href: "/ui/system_logs.html", label: "系统-日志" });
    items.push({ href: "/ui/system_config.html", label: "系统-配置" });
  } else if (role === "employee") {
    items.push({ href: "/ui/employee_profile.html", label: "我的信息" });
    items.push({ href: "/ui/employee_change_requests.html", label: "信息修改申请" });
    items.push({ href: "/ui/employee_overtime.html", label: "加班申请" });
    items.push({ href: "/ui/attendance_employee.html", label: "我的出勤" });
    items.push({ href: "/ui/salary_employee.html", label: "我的薪资" });
  }
  const left = el("div", { class: "nav-left" }, items.map(i => el("a", { href: i.href, class: "nav-link", text: i.label })));
  const right = el("div", { class: "nav-right" }, []);
  if (getToken()) {
    right.appendChild(el("span", { class: "nav-user", text: `${_getItem("user_id") || ""} (${role || "?"})` }));
    const btn = el("button", { class: "btn", type: "button", text: "退出" });
    btn.addEventListener("click", async () => {
      try {
        await api("/auth/logout", { method: "POST" });
      } catch (_) {
      } finally {
        clearAuth();
        window.location.href = "/ui/login.html";
      }
    });
    right.appendChild(btn);
  } else {
    right.appendChild(el("a", { href: "/ui/login.html", class: "nav-link", text: "登录" }));
  }
  nav.appendChild(left);
  nav.appendChild(right);
}

function requireLogin() {
  if (!getToken()) {
    window.location.href = "/ui/login.html";
    return false;
  }
  return true;
}

function requireRole(allowed) {
  if (!requireLogin()) return false;
  const role = getRole();
  if (!allowed.includes(role)) {
    window.location.href = "/ui/index.html";
    return false;
  }
  return true;
}

async function ensureMe() {
  if (!getToken()) return null;
  try {
    const me = await api("/auth/me");
    const token = getToken();
    if (token) {
      setAuth(token, me.user_id || _getItem("user_id") || "", me.role || _getItem("role") || "");
    }
    return me;
  } catch (e) {
    clearAuth();
    return null;
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  _getTabId();
  await ensureMe();
  renderNav();
});
