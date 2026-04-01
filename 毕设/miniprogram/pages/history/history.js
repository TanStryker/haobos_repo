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
      wx.redirectTo({ url: "/pages/login/login" });
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

