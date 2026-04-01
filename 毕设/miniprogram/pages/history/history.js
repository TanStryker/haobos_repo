const { request } = require("../../utils/api");

Page({
  data: {
    items: [],
    loading: false,
    msg: "",
    msgType: "hint"
  },
  async onShow() {
    const token = wx.getStorageSync("token");
    const role = wx.getStorageSync("role");
    if (!token || role !== "employee") {
      wx.removeStorageSync("token");
      wx.removeStorageSync("role");
      wx.removeStorageSync("user_id");
      if (this._navLock) return;
      this._navLock = true;
      wx.reLaunch({ url: "/pages/login/login?msg=" + encodeURIComponent("请先登录员工账号") });
      return;
    }
    await this.load();
  },
  async load() {
    this.setData({ loading: true, msg: "", msgType: "hint" });
    try {
      const data = await request("/me/attendance/punches?limit=5");
      this.setData({ items: data.items || [], msg: "加载成功", msgType: "ok" });
    } catch (e) {
      this.setData({ msg: e.message, msgType: "error" });
    } finally {
      this.setData({ loading: false });
    }
  }
});
