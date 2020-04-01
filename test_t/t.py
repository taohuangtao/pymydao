from pymydao import db_helper
print(1)
dbh = db_helper.DbHelper("172.16.1.78", "root", "new-password", "yuqing")
um = dbh.get_model_instance("user")
ll = um.select("SELECT * FROM `user` LIMIT 1")
print(ll)


def t2():
    dbh = db_helper.DbHelper("172.16.1.78", "root", "new-password", "yuqing")
    um = dbh.get_model_instance("user")
    ll = um.select("SELECT * FROM `user` LIMIT 1")
    print(ll)
