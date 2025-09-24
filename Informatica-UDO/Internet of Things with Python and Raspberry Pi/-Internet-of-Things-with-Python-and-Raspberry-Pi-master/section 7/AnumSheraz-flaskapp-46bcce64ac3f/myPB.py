from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub

server_UUID = "server"
cipherKey = "myCipherKey"
myChannel = "RSPY"

############################
pnconfig = PNConfiguration()

pnconfig.subscribe_key = 'sub-c-c96cd480-3528-11e8-a218-f214888d2de6'
pnconfig.publish_key = 'pub-c-f141a42f-ae6d-4f11-bbaf-4bc7cb518b6c'
pnconfig.secret_key = "sec-c-YzhiNDQ0ZDEtODNmMS00M2NmLTgwMDktZmNlYjgzMDJkYjE5"
pnconfig.uuid = server_UUID
pnconfig.cipher_key = cipherKey
pnconfig.ssl = True
pubnub = PubNub(pnconfig)


def grantAccess(auth_key, read, write):
    if read is True and write is True:
        grantReadAndWriteAccess(auth_key)
    elif read is True:
        grantReadAccess(auth_key)
    elif write is True:
        grantWriteAccess(auth_key)
    else:
        revokeAccess(auth_key)


def grantReadAndWriteAccess(auth_key):
    v = pubnub.grant() \
        .read(True) \
        .write(True) \
        .channels(myChannel) \
        .auth_keys(auth_key) \
        .ttl(60) \
        .sync()
    print("------------------------------------")
    print("--- Granting Read & Write Access ---")
    for key, value in v.status.original_response.iteritems():
        print(key, ":", value)
    print("------------------------------------")


def grantReadAccess(auth_key):
    v = pubnub.grant() \
        .read(True) \
        .channels(myChannel) \
        .auth_keys(auth_key) \
        .ttl(60) \
        .sync()
    print("------------------------------------")
    print("--- Granting Read Access ---")
    for key, value in v.status.original_response.iteritems():
        print(key, ":", value)
    print("------------------------------------")


def grantWriteAccess(auth_key):
    v = pubnub.grant() \
        .write(True) \
        .channels(myChannel) \
        .auth_keys(auth_key) \
        .ttl(60) \
        .sync()
    print("------------------------------------")
    print("--- Granting Write Access ---")
    for key, value in v.status.original_response.iteritems():
        print(key, ":", value)
    print("------------------------------------")


def revokeAccess(auth_key):
    v = pubnub.revoke() \
        .channels(myChannel) \
        .auth_keys(auth_key) \
        .sync()
    print("------------------------------------")
    print("--- Revoking Access ---")
    for key, value in v.status.original_response.iteritems():
        print(key, ":", value)
    print("------------------------------------")
