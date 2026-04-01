def main():
    try:
        import uvicorn
    except Exception as e:
        raise SystemExit("缺少依赖 uvicorn，请先安装：pip install fastapi uvicorn") from e

    uvicorn.run("hrms.main:app", host="127.0.0.1", port=8000, reload=True, app_dir=".")


if __name__ == "__main__":
    main()
