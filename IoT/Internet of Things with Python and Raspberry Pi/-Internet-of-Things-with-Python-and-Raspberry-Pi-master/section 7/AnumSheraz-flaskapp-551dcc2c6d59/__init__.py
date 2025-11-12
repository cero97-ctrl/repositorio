from flask import Flask, render_template, redirect, url_for, session, flash
from flask_dance.contrib.facebook import make_facebook_blueprint, facebook
from functools import wraps
from flask_sqlalchemy import SQLAlchemy
import myDB, myPB
import hashlib, string, random

import json, time

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:hello-world123@localhost/iotstorage'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

alive = 0
data = {}

# Paste in your facebook app ID and secret
facebookId = "180844045961238"
facebookSecret = "da8512c089189c459af393ab7df8e37b"

facebook_blueprint = make_facebook_blueprint(client_id=facebookId, client_secret=facebookSecret)
app.register_blueprint(facebook_blueprint, url_prefix='/facebook_login')

# grant read and write permissions to authKey "raspberry-pi"
myPB.grantAccess("raspberry-pi", True, True)


@app.route('/facebook_login')
def facebook_login():
    if not facebook.authorized:
        return redirect(url_for('facebook.login'))

    account_info = facebook.get('/me')
    if account_info.ok:
        print("access_token: ", facebook.access_token)
        me = account_info.json()
        session['logged_in'] = True
        session['facebook_token'] = facebook.access_token
        session['user'] = me['name']
        session['user_id'] = me['id']
        return redirect(url_for('main'))

    return redirect(url_for('login'))


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "logged_in" in session:
            if session["logged_in"]:
                return f(*args, **kwargs)

        flash("please login first !")
        return redirect(url_for('login'))

    return wrapper


def clear_user_session():
    session['logged_in'] = None
    session['facebook_token'] = None
    session['user'] = None
    session['user_id'] = None


def str_to_bool(s):
    if 'true' in str(s):
         return True
    elif 'false' in str(s):
         return False
    else:
         raise ValueError


def salt(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def createAuthKey():
    s = salt(10)
    hashing = hashlib.sha256(str(session['facebook_token']) + s)
    return hashing.hexdigest()


@app.route('/', methods=['POST', 'GET'])
def login():
    clear_user_session()
    return render_template('login.html')


@app.route("/logout")
@login_required  # you cannot logout if you're already not logged in!
def logout():
    myDB.userLogout(session["user_id"])
    myDB.viewAll()
    clear_user_session()
    flash("you just logged out!")
    return redirect(url_for('login'))


@app.route('/main')
@login_required
def main():
    session['keep_alive'] = 0
    flash(session['user'])
    myDB.addUserAndLogin(session['user'], int(session['user_id']))
    myDB.viewAll()
    return render_template('index.html', user_id=session['user_id'], online_users=myDB.getAllLoggedInUsers())


@app.route('/keep_alive', methods=['GET'])
def keep_alive():
    global data
    session['keep_alive'] += 1
    data['keep_alive'] = str(session['keep_alive'])
    parsed_json = json.dumps(data)
    print(parsed_json + ", user:" + session["user"])
    return str(parsed_json)


@app.route('/grant-<who>-<keyOrId>-<read>-<write>', methods=['POST', 'GET'])
def grant_access(who, keyOrId, read, write):
    if int(session['user_id']) == 10214511884608981:
        if who == "user":
            print("granting " + keyOrId + " read:" + read + ", write:" + write + " permission")
            myDB.addUserPermission(keyOrId, read, write)
            auth_key = myDB.getAuthKey(keyOrId)
            myPB.grantAccess(auth_key, str_to_bool(read), str_to_bool(write))
        elif who == "device":
            myPB.grantAccess(keyOrId, str_to_bool(read), str_to_bool(write))
    else:
        print("WHO ARE YOU ?")
        return json.dumps({"access": "denied"})
    return json.dumps({"access": "granted"})


@app.route('/get_authKey', methods=['POST', 'GET'])
def getAuthKey():
    print("Creating AuthKey for " + session['user'])
    auth_key = createAuthKey()
    myDB.addAuthKey(int(session['user_id']), auth_key)
    (read, write) = myDB.getUserAccess(int(session['user_id']))
    myPB.grantAccess(auth_key, read, write)
    authResponse = {"authKey": auth_key, "cipherKey": myPB.cipherKey}
    jsonResponse = json.dumps(authResponse)
    return str(jsonResponse)


if __name__ == '__main__':
    app.run(host="localhost", port=80)
