const { request } = require("../../utils/api");

function pad2(n) {
  return String(n).padStart(2, "0");
}

function nowIsoLocal() {
  const d = new Date();
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}T${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
}

function parseHHMM(s) {
  const parts = String(s || "").split(":");
  if (parts.length !== 2) return null;
  const h = Number(parts[0]);
  const m = Number(parts[1]);
  if (!Number.isFinite(h) || !Number.isFinite(m)) return null;
  if (h < 0 || h > 23 || m < 0 || m > 59) return null;
  return h * 60 + m;
}

function minutesOfDay(d) {
  return d.getHours() * 60 + d.getMinutes();
}

function haversineM(lat1, lng1, lat2, lng2) {
  const r = 6371000;
  const p1 = (lat1 * Math.PI) / 180;
  const p2 = (lat2 * Math.PI) / 180;
  const dp = ((lat2 - lat1) * Math.PI) / 180;
  const dl = ((lng2 - lng1) * Math.PI) / 180;
  const a = Math.sin(dp / 2) ** 2 + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return r * c;
}

function validatePunch(rule, dt, lat, lng) {
  if (!rule || !rule.enabled) return { ok: false, reason: "未启用打卡规则" };
  const startMin = parseHHMM(rule.start_time);
  const endMin = parseHHMM(rule.end_time);
  if (startMin == null || endMin == null) return { ok: false, reason: "打卡规则时间配置错误" };
  const nowMin = minutesOfDay(dt);
  if (nowMin < startMin || nowMin > endMin) return { ok: false, reason: "不在规定打卡时间范围内" };
  if (rule.center_lat == null || rule.center_lng == null || rule.allowed_radius_m == null) return { ok: true, reason: "" };
  if (lat == null || lng == null) return { ok: false, reason: "未获取到定位信息" };
  const dist = haversineM(Number(lat), Number(lng), Number(rule.center_lat), Number(rule.center_lng));
  if (dist > Number(rule.allowed_radius_m)) return { ok: false, reason: "不在规定打卡地点范围内" };
  return { ok: true, reason: "" };
}

function validatePunchAny(rules, dt, lat, lng) {
  if (!Array.isArray(rules) || rules.length === 0) return { ok: false, reason: "未配置可用打卡规则" };
  const reasons = [];
  for (const r of rules) {
    const v = validatePunch(r, dt, lat, lng);
    if (v.ok) return { ok: true, reason: "" };
    if (v.reason && !reasons.includes(v.reason)) reasons.push(v.reason);
  }
  if (reasons.length === 0) return { ok: false, reason: "不符合任何已配置规则" };
  return { ok: false, reason: "不符合任何已配置规则：" + reasons.join("；") };
}

async function reverseGeocode(lat, lng) {
  try {
    const data = await request(`/geo/reverse?lat=${encodeURIComponent(lat)}&lng=${encodeURIComponent(lng)}`);
    const addr = data && data.address ? String(data.address).trim() : "";
    return addr;
  } catch (e) {
    return "";
  }
}

Page({
  data: {
    userId: "",
    ruleLoaded: false,
    workType: "",
    rules: [],
    loading: false,
    msg: "",
    msgType: "hint"
  },
  async onShow() {
    const token = wx.getStorageSync("token");
    const role = wx.getStorageSync("role");
    const userId = wx.getStorageSync("user_id");
    if (!token || role !== "employee") {
      wx.removeStorageSync("token");
      wx.removeStorageSync("role");
      wx.removeStorageSync("user_id");
      if (this._navLock) return;
      this._navLock = true;
      wx.reLaunch({ url: "/pages/login/login?msg=" + encodeURIComponent("请使用员工账号登录") });
      return;
    }
    this.setData({ userId: userId || "" });
    await this.loadRules();
  },
  async loadRules() {
    this.setData({ ruleLoaded: false });
    try {
      const data = await request("/attendance/rules");
      this.setData({ ruleLoaded: true, workType: data.work_type || "", rules: data.rules || [] });
    } catch (e) {
      this.setData({ ruleLoaded: true, workType: "", rules: [], msg: e.message, msgType: "error" });
    }
  },
  onLogout() {
    wx.removeStorageSync("token");
    wx.removeStorageSync("role");
    wx.removeStorageSync("user_id");
    if (this._navLock) return;
    this._navLock = true;
    wx.reLaunch({ url: "/pages/login/login" });
  },
  toHistory() {
    wx.navigateTo({ url: "/pages/history/history" });
  },
  async onChooseAndPunch() {
    this.setData({ loading: true, msg: "", msgType: "hint" });
    try {
      const loc = await new Promise((resolve, reject) => {
        wx.getLocation({
          type: "gcj02",
          isHighAccuracy: true,
          highAccuracyExpireTime: 3000,
          success: resolve,
          fail: (err) => reject(new Error(err.errMsg || "获取定位失败"))
        });
      });
      const lat = loc.latitude;
      const lng = loc.longitude;
      const ts = nowIsoLocal();
      const dt = new Date();
      const v = validatePunchAny(this.data.rules, dt, lat, lng);
      if (!v.ok) {
        this.setData({ msg: `无效打卡：${v.reason}`, msgType: "error" });
        return;
      }
      const address = (await reverseGeocode(lat, lng)) || "GPS定位";
      await request("/me/attendance/punch", {
        method: "POST",
        header: { "Content-Type": "application/json" },
        data: { ts, address, lat, lng }
      });
      this.setData({ msg: "有效打卡：已提交", msgType: "ok" });
    } catch (e) {
      const msg = String(e && e.message ? e.message : "");
      if (msg.includes("auth deny") || msg.includes("authorize") || msg.includes("权限") || msg.includes("拒绝")) {
        this.setData({ msg: "定位权限未开启，请在设置中允许小程序使用定位权限", msgType: "error" });
        try {
          await new Promise((resolve) => wx.openSetting({ success: resolve, fail: resolve }));
        } catch (_) {
        }
      } else {
        this.setData({ msg: msg || "操作失败", msgType: "error" });
      }
    } finally {
      this.setData({ loading: false });
    }
  }
});
