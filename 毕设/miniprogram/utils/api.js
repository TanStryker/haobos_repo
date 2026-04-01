function getBaseUrl() {
  const app = getApp();
  return (app && app.globalData && app.globalData.baseUrl) || "";
}

function getToken() {
  return wx.getStorageSync("token") || "";
}

function request(path, options) {
  const baseUrl = getBaseUrl();
  const token = getToken();
  const headers = Object.assign({}, (options && options.header) || {});
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }
  const url = baseUrl + path;
  return new Promise((resolve, reject) => {
    wx.request({
      url,
      method: (options && options.method) || "GET",
      header: headers,
      data: (options && options.data) || undefined,
      success(res) {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(res.data);
          return;
        }
        const msg = (res.data && res.data.detail) || "请求失败";
        reject(new Error(msg));
      },
      fail(err) {
        reject(new Error(err.errMsg || "网络错误"));
      }
    });
  });
}

module.exports = { request };

