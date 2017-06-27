from flask import render_template

from app import app

@app.route('/')
@app.route('/index')
def index():
	return render_template("index.html")


@app.route('/4c')
def cfg():
	return render_template("4c.html")


@app.route('/3c')
def dfg():
        return render_template("3c.html")
