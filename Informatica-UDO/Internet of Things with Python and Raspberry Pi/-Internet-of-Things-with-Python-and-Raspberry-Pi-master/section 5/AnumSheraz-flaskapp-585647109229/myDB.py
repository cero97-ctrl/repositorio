from flask_sqlalchemy import SQLAlchemy

from __init__ import db


class userTable(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(4096))
    user_id = db.Column(db.Integer)
    authkey = db.Column(db.String(4096))
    login = db.Column(db.Integer)
    read_access = db.Column(db.Integer)
    write_access = db.Column(db.Integer)

    def __init__(self, name, user_id, authkey, login, read_access, write_access):
        self.name = name
        self.user_id = user_id
        self.authkey = authkey
        self.login = login
        self.read_access = read_access
        self.write_access = write_access


def delete_all():
    try:
        db.session.query(userTable).delete()
        db.session.commit()
        Print("Delete All Done!")
    except Exception as e:
        print("failed " + str(e))
        db.session.rollback()


def getUserRowIfExists(user_id):
    get_user_row = userTable.query.filter_by(user_id=user_id).first()
    if (get_user_row != None):
        return get_user_row
    else:
        print("user doesn't exists")
        return False


def addUserAndLogin(name, user_id):
    row = getUserRowIfExists(user_id)
    if (row != False):
        row.login = 1
        db.session.commit()
    else:
        print("adding user " + name)
        new_user = userTable(name, user_id, None, 1, 0, 0)
        db.session.add(new_user)
        db.session.commit()
    print("user " + name + " login added")


def userLogout(user_id):
    row = getUserRowIfExists(user_id)
    if (row != False):
        row.login = 0
        db.session.commit()
        print("user " + row.name + " logout Updated")


def addAuthKey(user_id, auth):
    row = getUserRowIfExists(user_id)
    if (row != False):
        row.authkey = auth
        db.session.commit()
        print("user " + row.name + " authkey added")


def viewAll():
    row = userTable.query.all()
    for n in range(0, len(row)):
        print(str(row[n].id) + " | " +
              row[n].name + " | " +
              str(row[n].user_id) + " | " +
              str(row[n].authkey) + " | " +
              str(row[n].login))


def getAllLoggedInUsers():
    row = userTable.query.filter_by(login=1).all()
    print("LoggedIn Users:")
    for n in range(0, len(row)):
        print(str(row[n].id) + " | " +
              row[n].name + " | " +
              str(row[n].user_id) + " | " +
              str(row[n].authkey) + " | " +
              str(row[n].login))
