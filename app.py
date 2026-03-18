from api import create_app, get_debug_mode

app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=get_debug_mode())
