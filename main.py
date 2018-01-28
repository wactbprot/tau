import couchdb as cdb
import argparse
import sys
import os
import json
import logging
import coloredlogs


class Tau(object):
    """docstring for tau."""
    def __init__(self):
        self.log = logging.getLogger(__name__)
        coloredlogs.install(level='DEBUG', logger=self.log)

        parser = argparse.ArgumentParser()
        ## conf file
        parser.add_argument("--conf", type = str, nargs = 1,
                            help = """file containing database and repo
                            params. Defaults to conf.json """)
        ## ini
        parser.add_argument('-i', action='store_true'
                            , help="""clones all documents in the
                            database db.name. Saves last change"""
                            , default=False)
        ## update
        parser.add_argument('-u', action='store_true'
                            , help="""updates all files on file system
                            since the last execution of tau -u """
                            , default=False)

        self.args =  parser.parse_args()

        if self.args.conf:
            conf_file = self.args.file[0]
        else:
            conf_file = "./conf.json"

        ## open and parse config file
        with open(conf_file) as json_conf:
            self.conf = json.load(json_conf)

        srv               = cdb.Server(self.conf['db']['url'])
        self.db_name      = self.conf['db']['name']
        self.db           = srv[self.db_name]

        self.base_folder  = self.conf["repo"]["folder"]
        self.db_folder    = "{}/{}".format(self.base_folder, self.db_name)
        self.doc_folder   = "{}/{}".format(self.db_folder, "db_docs")
        self.changes_file = "{}/changes.json".format(self.db_folder)

    def store_changes(self):
        ch =  self.db["_changes"]

        with open(self.changes_file, 'w') as f:
            json.dump(ch, f, indent=2)

    def store_doc(self, id):
        fn = "{}/{}.json".format(self.doc_folder, id)
        with open(fn, 'w') as of:
            json.dump(self.db[id], of, indent=2)

            self.log.info("""document {} stored""".format( fn ))

    def gen_folder(self):
        if not os.path.exists(self.base_folder):
            os.makedirs(self.base_folder)
            self.log.debug("""generated foler {}""".format(self.base_folder))

        if not os.path.exists(self.db_folder):
            os.makedirs(self.db_folder)
            self.log.debug("""generated foler {}""".format(self.db_folder))

        if not os.path.exists(self.doc_folder):
            os.makedirs(self.doc_folder)
            self.log.debug("""generated foler {}""".format(self.doc_folder))

    def ini(self):
        self.gen_folder()
        for id in self.db:
            self.store_doc( id )

        self.store_changes()


    def update(self):
        with open(self.changes_file) as ch:
            changes = json.load(ch)

        change_set_old  = changes["results"]
        change_set_new  = self.db["_changes"]["results"]

        for i, change in enumerate(change_set_new):
            if i in change_set_old:
                if change["seq"] != change_set_old[i]["seq"]:
                    self.store_doc( change["id"])
            else:
                self.store_doc( change["id"])

        self.store_changes()

def main():
    """ Parses the command line options and calls the methods.
    """

    tau = Tau()

    if tau.args.i:
        tau.ini()

    if tau.args.u:
        tau.update()

if __name__ == "__main__":
    main()
