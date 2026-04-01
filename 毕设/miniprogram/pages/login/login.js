const { request } = require("../../utils/api");

Page({
  data: {
    userId: "",
    password: "",
    loading: false,
    msg: "",
    msgType: "hint"
  },
  onShow() {
    const token = wx.getStorageSync("token");
    if (token) {
      wx.redirectTo({ url: "/pages/punch/punch" });
    }
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
      wx.redirectTo({ url: "/pages/punch/punch" });
    } catch (e) {
      this.setData({ msg: e.message, msgType: "error" });
    } finally {
      this.setData({ loading: false });
    }
  }
});

