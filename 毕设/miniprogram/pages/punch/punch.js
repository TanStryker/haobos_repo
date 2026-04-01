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

Page({
  data: {
    userId: "",
    ruleLoaded: false,
    rule: {},
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
    await this.loadRule();
  },
  async loadRule() {
    this.setData({ ruleLoaded: false });
    try {
      const rule = await request("/attendance/rule");
      this.setData({ ruleLoaded: true, rule });
    } catch (e) {
      this.setData({ ruleLoaded: true, rule: {}, msg: e.message, msgType: "error" });
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
        wx.chooseLocation({
          success: resolve,
          fail: (err) => reject(new Error(err.errMsg || "获取位置失败"))
        });
      });
      const address = (loc.address || "") + (loc.name ? ` ${loc.name}` : "");
      const lat = loc.latitude;
      const lng = loc.longitude;
      const ts = nowIsoLocal();
      const dt = new Date();
      const v = validatePunch(this.data.rule, dt, lat, lng);
      if (!v.ok) {
        this.setData({ msg: `无效打卡：${v.reason}`, msgType: "error" });
        return;
      }
      await request("/me/attendance/punch", {
        method: "POST",
        header: { "Content-Type": "application/json" },
        data: { ts, address, lat, lng }
      });
      this.setData({ msg: "有效打卡：已提交", msgType: "ok" });
    } catch (e) {
      this.setData({ msg: e.message, msgType: "error" });
    } finally {
      this.setData({ loading: false });
    }
  }
});
