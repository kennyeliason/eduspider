from flask import Flask, render_template
import db

app = Flask(__name__)


@app.route("/")
def index():
    topics = db.get_topics()
    return render_template("index.html", topics=topics)


@app.route("/topic/<name>")
def topic(name):
    pages = db.get_pages_for_topic(name)
    return render_template("topic.html", topic_name=name, pages=pages)


@app.route("/crawls")
def crawls():
    crawl_list = db.get_crawls()
    return render_template("crawl_status.html", crawls=crawl_list)


if __name__ == "__main__":
    db.init_db()
    app.run(host="0.0.0.0", port=5555, debug=True)
