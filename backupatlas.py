#!/usr/bin/env python
from __future__ import print_function

import datetime
import os
import sys
import glob
import tarfile
import shutil

import pymssql

import settings

now = datetime.datetime.now()


def collect_files(loc):
    return glob.glob(loc)


def atlas():
    conn = pymssql.connect(**settings.atlasdb)

    conn.autocommit(True)

    cur = conn.cursor()

    cur.execute('''
    BACKUP DATABASE %s TO  DISK = N'%s'
    WITH NOFORMAT, INIT,  NAME = N'Atlas_%s', SKIP, NOREWIND,
    NOUNLOAD,  STATS = 10
    ''' % (
        settings.atlasdb['database'], settings.atlaswindowsbackuploc,
        now.strftime('%d%m%Y'))
    )

    conn.autocommit(False)

    conn.close()

    return collect_files(settings.atlaslocalbackuploc)


def sjukerfi():
    return collect_files(settings.sjukerfilocalbackuploc)


def sameign():
    return collect_files(settings.sameignlocalbackuploc)


def do_backup(filelist, prefix=''):
    to = os.path.join(
        settings.localbackuploc, str(now.year), str(now.month), str(now.day)
    )
    if not os.path.exists(to):
        os.makedirs(to)
    to = os.path.join(to, '%s%s.tar.gz' % (prefix, now.strftime('%H%M%S')))
    with tarfile.open(to, 'w:gz') as tf:
        for bf in filelist:
            arcname = os.path.split(bf)[-1]
            tf.add(bf, arcname=arcname)
            if not os.path.isdir(bf):
                shutil.copyfile(
                    bf, os.path.join(settings.secondary_collect, arcname)
                )


def error(*args):
    print(*args, file=sys.stderr)

if __name__ == '__main__':
    arg = sys.argv[-1]
    avail = ('all', 'atlas', 'sjukerfi', 'sameign')
    if len(sys.argv) != 2 or arg not in avail:
        print('Usage: ./backupatlas.py [all|atlas|sjukerfi|sameign]')
    else:
        print('Running at %s' % datetime.datetime.now().strftime('%d.%m.%Y'))
        f1 = f2 = f3 = []
        # if arg in ('atlas', 'all'):
        if 0:
            f1 = atlas()
            if not f1:
                error('Did not find any files for ATLAS backup')
        if arg in ('sjukerfi', 'all'):
            f2 = sjukerfi()
            if not f2:
                error('Did not find any files for SJUKERFI backup')
        if arg == 'sameign':
            f3 = sameign()
            do_backup(f3, prefix='sameign')
            if not f3:
                error('Did not find any files for SAMEIGN backup')
        if f1 or f2:
            do_backup(f1+f2)
