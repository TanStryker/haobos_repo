const { request } = require("../../utils/api");

Page({
  data: {
    userId: "",
    password: "",
    loading: false,
    msg: "",
    msgType: "hint"
  },
  onLoad(options) {
    const msg = (options && options.msg) ? decodeURIComponent(options.msg) : "";
    if (msg) {
      this.setData({ msg, msgType: "error" });
    }
  },
  onShow() {
    const token = wx.getStorageSync("token");
    const role = wx.getStorageSync("role");
    if (!token) return;
    if (role === "employee") {
      if (this._navLock) return;
      this._navLock = true;
      wx.reLaunch({ url: "/pages/punch/punch" });
      return;
    }
    wx.removeStorageSync("token");
    wx.removeStorageSync("role");
    wx.removeStorageSync("user_id");
    this.setData({ msg: "请使用员工账号登录", msgType: "error" });
  },
  onUserId(e) {
    this.setData({ userId: e.detail.value });
  },
  onPassword(e) {
    this.setData({ password: e.detail.value });
  },
  async onLogin() {
    this.setData({ loading: true, msg: "", msgType: "hint" });
    try {
      const data = await request("/auth/login", {
        method: "POST",
        header: { "Content-Type": "application/json" },
        data: { user_id: this.data.userId.trim(), password: this.data.password }
      });
      wx.setStorageSync("token", data.token);
      wx.setStorageSync("user_id", data.user_id);
      wx.setStorageSync("role", data.role);
      this.setData({ msg: "登录成功", msgType: "ok" });
      if (data.role !== "employee") {
        wx.removeStorageSync("token");
        wx.removeStorageSync("role");
        wx.removeStorageSync("user_id");
        this.setData({ msg: "当前账号不是员工，无法打卡", msgType: "error" });
        return;
      }
      if (this._navLock) return;
      this._navLock = true;
      wx.reLaunch({ url: "/pages/punch/punch" });
    } catch (e) {
      this.setData({ msg: e.message, msgType: "error" });
    } finally {
      this.setData({ loading: false });
    }
  }
});
