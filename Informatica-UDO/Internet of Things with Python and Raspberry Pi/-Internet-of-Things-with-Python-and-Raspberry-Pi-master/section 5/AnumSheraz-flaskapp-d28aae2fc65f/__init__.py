from flask import Flask, render_template, redirect, url_for, session, flash
from flask_dance.contrib.facebook import make_facebook_blueprint, facebook

import json, time

app = Flask(__name__)

alive = 0
data = {}

#Paste in your facebook app ID and secret
facebookId = "180844045961238"
facebookSecret = "da8512c089189c459af393ab7df8e37b"

facebook_blueprint = make_facebook_blueprint(client_id=facebookId, client_secret=facebookSecret)
app.register_blueprint(facebook_blueprint, url_prefix='/facebook_login')

@app.route('/facebook_login')
def facebook_login():
    if not facebook.authorized:
        return redirect(url_for('facebook.login'))

    account_info = facebook.get('/me')
    if account_info.ok:
        print("access_token: ", facebook.access_token)
        me = account_info.json()
        session['facebook_token'] = facebook.access_token
        session['user'] = me['name']
        session['user_id'] = me['id']
        return redirect(url_for('main'))

    return redirect(url_for('login'))

@app.route('/', methods=['POST', 'GET'])
def login():
    session['facebook_token'] = None
    session['user'] = None
    session['user_id'] = None
    return render_template('login.html')

@app.route("/logout")
def logout():
    session['facebook_token'] = None
    session['user'] = None
    session['user_id'] = None
    flash("you just logged out!")
    return redirect(url_for('login'))

@app.route('/main')
def main():
    flash(session['user'])
    return render_template('index.html')

@app.route('/keep_alive', methods=['GET'])
def keep_alive():
    global alive, data
    alive += 1
    keep_alive_count = str(alive)
    data['keep_alive'] = keep_alive_count
    parsed_json = json.dumps(data)
    print(parsed_json)
    return str(parsed_json)

if __name__ == '__main__':
   app.run(host="localhost", port=80)
