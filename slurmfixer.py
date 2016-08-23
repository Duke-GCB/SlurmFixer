import sys
import re
import pymysql
import argparse
import subprocess

DB_CONFIG_FILENAME = '/etc/slurm/slurmdbd.conf'
CLUSTER_NAME = 'hardac'
# list job IDS for currently running processes without printing a header
QUEUE_LIST_COMMAND = ['squeue', '-h', '-o', '"%A"']
NODE_LIST_COMMAND = ['sinfo', '-h', '-N', '-o', '%N']
EXPAND_NODE_NAMES_BASE_COMMAND = ['scontrol', 'show', 'hostname']
SKIP_USERS = [
    'root', 
    'postfix',
    'ntp',
    'rpc',
    'rpcuser',
    'dbus',
    'munge',
    'ganglia',
    'nscd',
    '68',
]

class Config(object):
    """
    Parses slurmdbd.conf config file.
    """

    def __init__(self, filename):
        with open(filename, 'r') as infile:
            config_dict = Config.get_config_dictionary(infile)
            self.host = config_dict['StorageHost']
            self.port = config_dict['StoragePort']
            self.user = config_dict['StorageUser']
            self.password = config_dict['StoragePass']
            self.db_name = config_dict['StorageLoc']

    @staticmethod
    def get_config_dictionary(infile):
        result = {}
        for line in infile.readlines():
            line = line.strip()
            if line.startswith("#") or not line:
                continue
            parts = line.split("=")
            key = parts[0]
            value = parts[1]
            result[key] = value
        return result


def get_db_connection(config_filename):
    config = Config(config_filename)
    port = None
    if config.port:
        port = int(config.port)
    return pymysql.connect(host=config.host,
                           user=config.user,
                           password=config.password,
                           db=config.db_name,
                           port=port,
                           cursorclass=pymysql.cursors.DictCursor)


def find_unfinished_jobs(db):
    with db.cursor() as cursor:
        # Read a single record
        sql = "select *, from_unixtime(time_start) as start " \
              " from {}_job_table where state < 3 or time_end = 0" \
              " order by time_start".format(CLUSTER_NAME)
        cursor.execute(sql)
        result = cursor.fetchall()
    return result


def find_running_jobs():
    lines = subprocess.check_output(QUEUE_LIST_COMMAND).decode("utf-8")
    return [int(line.replace('"','')) for line in lines.split("\n") if line]


def find_bad_jobs(db):
    bad_jobs = []
    running_jobs = set(find_running_jobs())
    for job in find_unfinished_jobs(db):
        job_id = job['id_job']
        if not job_id in running_jobs:
            bad_jobs.append(job)
    return bad_jobs


def find_bad():
    db = get_db_connection(DB_CONFIG_FILENAME)
    print_bad_job_line("JOBID", "STARTED", "ACCOUNT", "USERID", "STATE", "JOB NAME")
    for job in find_bad_jobs(db):
        print_bad_job_line(str(job['id_job']), str(job['start']),
                           job['account'], str(job['id_user']),
                           str(job['state']), job['job_name'])


def print_bad_job_line(job_id, start, account, user_id, state, job_name):
    print(job_id.ljust(10), start.ljust(20), account.ljust(12), state.ljust(5), user_id.ljust(10), job_name)


def fix_bad():
    db = get_db_connection(DB_CONFIG_FILENAME)
    fix_bad_jobs(db)

def fix_bad_jobs(db):
    bad_jobs = find_bad_jobs(db)
    print("Fixing", len(bad_jobs), "jobs.")
    with db.cursor() as cursor:
        sql = "update {}_job_table " \
              " set state = 5, time_end = time_start + 1 " \
              " where id_job = %s ".format(CLUSTER_NAME)
        for job in bad_jobs:
            print("Run:", sql, job['id_job'])
        # Read a single record
        #cursor.execute(sql)
        #result = cursor.fetchall()

def find_orphans():
    node_names = get_node_names()
    running_user_node_names = set(get_running_user_node_names())
    print_orphan("NODE", "USER", "PID", "CMD")
    for node_name in node_names:
        try:
            for user, pid, cmd in get_node_processes(node_name):
               user_node_name = "{}|{}".format(user,node_name)
               if not user_node_name in running_user_node_names:
                  print_orphan(node_name, user, pid, cmd)
        except:
            sys.stderr.write("Failed to check node {}\n.".format(node_name))

def print_orphan(node_name, user, pid, cmd):
    print(node_name.ljust(20), user.ljust(10), pid.ljust(8), cmd)

def get_node_names():
    node_names = []
    compressed_names_str = subprocess.check_output(NODE_LIST_COMMAND).decode("utf-8").strip()
    for compressed_name in name_string_to_list(compressed_names_str):
       expand_node_names_command = EXPAND_NODE_NAMES_BASE_COMMAND[:]
       expand_node_names_command.append(compressed_name)
       expanded = subprocess.check_output(expand_node_names_command).decode("utf-8").strip()
       node_names.extend(expanded.split('\n'))
    return node_names
    #return subprocess.check_output(expand_node_names_command).decode("utf-8")

    #EXPAND_NODE_NAMES_BASE_COMMAND = ['scontrol', 'show', 'hostname']

def get_node_processes(node_name):
    node_processes = []
    ps_command = ["ssh", node_name, "ps", "-e", "--no-headers", "-o", "\"%U|%p|%a\""]
    lines = subprocess.check_output(ps_command).decode("utf-8").strip().split('\n')
    for line in lines:
        parts = line.strip().split('|')
        user = parts[0].strip()
        pid = parts[1].strip()
        cmd = parts[2].strip()
        if not user in SKIP_USERS:
            node_processes.append((user, pid, cmd))
    return node_processes
    

def name_string_to_list(compressed_names):
    splitter = re.compile(r'(?:[^,\[]|\[[^\]]*\])+')
    return splitter.findall(compressed_names)

def get_running_user_node_names():
    squeue_cmd = ["squeue", "-o", "%u|%N"]
    lines = subprocess.check_output(squeue_cmd).decode("utf-8").strip().split('\n')
    return lines

   

def add_sub_command(child_parsers, name, help, func):
    child_parser = child_parsers.add_parser(name, help=help)
    child_parser.set_defaults(func=func)


def main():
    parser = argparse.ArgumentParser()
    child_parsers = parser.add_subparsers(help='commands')
    add_sub_command(child_parsers, 'find_bad', 'Find bad jobs', find_bad)
    add_sub_command(child_parsers, 'fix_bad', 'Fix bad jobs', fix_bad)
    add_sub_command(child_parsers, 'find_orphans', 'Find orphaned processes', find_orphans)
    parsed_args = parser.parse_args()
    if hasattr(parsed_args, 'func'):
        parsed_args.func()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

