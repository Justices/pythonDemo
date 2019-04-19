import  itchat


@itchat.msg_register([itchat.content.TEXT, itchat.content.PICTURE])
def show_message(msg):
    print msg['Text']



itchat.auto_login(hotReload=True)

itchat.send(u'message send', 'filehelper')
itchat.run()
