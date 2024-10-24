#!/usr/bin/env python3
"""
an undo/redo log over an append only log

> do A, do B, undo B, redo B, undo B, redo B

we store a list of operations, and annotate them to
reconstruct the linear history

0. init
1. do A (do 1, prev 0)
2. do B (do 2, prev: 1)
3. undo B (do 1, prev: 0)
4. redo B (do 2, prev: 3)
5. undo B (do 1, prev: 0)
6. redo B (do 2, prev: 5)

we then split each operation into a `prepare` and
`commit`, that allows us to rollback incomplete
or interrupted operations

"""

def example_code():
    l = OpLog(FakeLog("test"), FakeStore("test"))
    l.init({"internal": False})

    with l.do("A") as txn:
        txn.set_store("foo", "A")

    with l.do("B") as txn:
        txn.set_store("bar", "B")

    with l.do("C") as txn:
        txn.set_store("foo", "C")

    with l.do("D") as txn:
        txn.set_store("bar", "D")

    with l.do("E") as txn:
        txn.set_store("foo", "E") # changes applied to FakeStore
        txn.set_store("bar", "E")

        txn.set_state("internal", True) ## stored inside the log

    l.undo()
    for r in l.redos():
        print("redo", r)
    l.redo()

    l.print()

    new_log = FakeLog("new")
    l.compact(new_log)
    l.print()

# ----

import json

from contextlib import contextmanager
from datetime import datetime, timezone

def now():
    return datetime.now(timezone.utc).timestamp()

class Bad(Exception):
    pass

class CancelTransaction(Exception):
    pass

class FakeLog:
    """ an in memory version of a log file"""
    def __init__(self, name):
        self.name = name
        self.i = []

    def entries(self):
        return self.i

    def get(self, idx):
        return self.i[idx]

    def top(self):
        return len(self.i)-1, self.i[-1]

    def append(self, op):
        self.i.append(op)

    def next_idx(self):
        return len(self.i)

class FakeStore:
    def __init__(self, fh):
        self.fh = fh
        self.d = {}

    def set(self, k, v):
        self.d[k] = v

    def get(self, k):
        return self.d.get(k)

    def apply(self, changes):
        for k in changes:
            old, new = changes[k]
            if self.d.get(k) != old:
                raise Bad("oh no")
            self.d[k] = new

    def rollback(self, changes):
        for k in changes:
            old, new = changes[k]
            current = self.d.get(k)
            if current == new:
                self.d[k] = old
            elif current != old:
                raise Bad("oh no")


class Operation:
    def __init__(self, n, kind, description, prev_idx=None, linear_idx=None, redos=(), state=None, changes=None, prepare_idx=None, date=None):
        self.kind = kind                # commit or prepare, for do, undo or redo
        self.description = description  # the description
        self.date = date                # date of operation

        self.n = n                      # n is the operation number in the linear history
        self.linear_idx = linear_idx    # the commit that was originally run to get here
        self.prev_idx = prev_idx        # the previous operation in the linear history

        self.state = state              # some state of the world, mutated by actions
        self.redos = redos              # a (linear_idx, last_redo_idx) list of operations to redo

        self.changes = changes          # in a prepare operation, this contains the changes to the store
        self.prepare_idx = prepare_idx  # in a commit operation, this points to the prepare

    def __str__(self):
        return f"{self.n} {self.kind: <14} {self.description: <5}\tlinear_idx={self.linear_idx}\t prev_idx={self.prev_idx}\tredos={self.redos}, state={self.state}, changes={self.changes}"

class Transaction:
    def __init__(self, description, state, store):
        self.description = description
        self.old_state = state
        self.new_state = dict(state)
        self.store = store
        self.changes = {}

    def set_store(self, key, value):
        if key in self.changes:
            old, new = self.changes[key]
        else:
            old = self.store.get(key)
        self.changes[key] = (old, value)

    def set_state(self, key, value):
        self.new_state[key] = value

    def cancel(self):
        raise CancelTransaction


class OpLog:
    def __init__(self, log, store):
        self.log = log
        self.store = store
        # self.linear = [] # used for correctness checking


    def init(self, state):
        if self.log.next_idx() == 0:
            init = Operation(n=0, kind="commit-init", description="", linear_idx=0, state=state, date=now())
            self.log.append(init)

    def state(self):
        top_idx, top = self.top()
        return top.state

    def history(self):
        return [f"{x.n} {x.kind}: {x.description}, {x.state}" for x in self.log.entries()]

    def linear_history(self):
        top_idx, top = self.log.top()
        if top_idx == 0 or top.linear_idx == 0:
            return []

        out = []
        while top.linear_idx > 0:
            linear_idx = self.log.get(top.linear_idx)

            action = f"{top.date} {linear_idx.description}"
            out.append(action)

            top = self.log.get(top.prev_idx)

        out.reverse()
        return out

    def recover(self):
        top_idx, top = self.log.top()

        if not top.kind.startswith("prepare-"):
            return

        prev_idx = top.prev_idx
        prev = self.log.get(prev_idx)

        date = now()

        rollback_entry = Operation(
            kind = top.kind.replace("prepare-", "rollback-"),
            description = top.description,
            date = date,

            n = prev.n,
            prev_idx = prev.prev_idx,
            linear_idx = prev.linear_idx,

            redos = prev.redos,
            state = prev.state,
            prepare_idx = top_idx,
        )

        changes = top.changes
        self.store.rollback(changes)
        self.log.append(rollback_entry)


    def compact(self, new_log):
        top_idx, top = self.log.top()

        init = self.log.get(0)
        new_log.append(init)

        if top_idx == 0 or top.linear_idx == 0:
            self.log = new_log
            return

        entries = [None] * (top.n)

        while top.linear_idx > 0:
            entries[top.n-1] = top
            top = self.log.get(top.prev_idx)

        prev_idx = 0

        for top in entries:
            linear_top = self.log.get(top.linear_idx)

            prepare = self.log.get(linear_top.prepare_idx)

            prepare_entry = Operation(
                kind = "prepare-do",
                description = linear_top.description,
                date = top.date,

                n = prepare.n,
                prev_idx = prev_idx,

                redos = (),
                state = top.state,
                changes = prepare.changes,
            )

            prepare_idx = new_log.next_idx()
            new_log.append(prepare_entry)


            linear_idx = new_log.next_idx()

            commit_entry = Operation(
                kind = "commit-do",
                description = linear_top.description,
                date = top.date,

                n = top.n,
                linear_idx = linear_idx,
                prev_idx = prev_idx,

                state = top.state,
                redos = (),
                prepare_idx = prepare_idx,
            )
            new_log.append(commit_entry)

            prev_idx = linear_idx

        linear_idx = self.log.next_idx()
        compact = Operation(
            kind="commit-close",
            description="",
            date=now(),

            n = top.n + 1,
            linear_idx = linear_idx,
            prev_idx = top.prev_idx,

            state=top.state,
            redos=(),
        )
        self.log.append(compact)

        self.log = new_log

    @contextmanager
    def do(self, description):
        top_idx, top = self.log.top()

        txn = Transaction(description, top.state, self.store)
        try:
            yield txn
        except CancelTransaction:
            return

        changes = txn.changes
        state = txn.new_state

        ## prepare Op

        date = now()

        prepare_entry = Operation(
            kind = "prepare-do",
            description = description,
            date = date,

            n = top.n+1,
            prev_idx = top_idx,

            state = state,
            changes = txn.changes,
        )

        prepare_idx = self.log.next_idx()
        self.log.append(prepare_entry)

        linear_idx = self.log.next_idx()

        commit_entry = Operation(
            kind = "commit-do",
            description = description,
            date = date,

            n = top.n+1,
            prev_idx = top_idx,
            linear_idx = linear_idx,

            state = state,
            prepare_idx = prepare_idx,
        )

        rollback_entry = Operation(
            kind = "rollback-do",
            description = description,
            date = date,

            n = top.n,
            prev_idx = top.prev_idx,
            linear_idx = top.linear_idx,

            redos = top.redos,
            state = top.state,
            prepare_idx = prepare_idx,
        )

        try:
            self.store.apply(changes)
        except:
            self.store.rollback(changes)
            self.log.append(rollback_entry)
            raise
        else:
            self.log.append(commit_entry)
            # self.linear.append(commit_entry.linear_idx)


    def redos(self):
        top_idx, top = self.log.top()

        out = []

        for (redo_idx, last_redo_idx) in top.redos:
            redo = self.log.get(redo_idx)
            last_redo = self.log.get(last_redo_idx)
            out.append(f"{last_redo.date} {redo.description}")

        return out


    def redo(self, n=-1):
        top_idx, top = self.log.top()
        top_redos = top.redos

        if len(top_redos) == 0:
            raise  Bad("redo: othing to redo, empty operation log")
        elif n < -len(top_redos)  or n >= len(top_redos):
            raise Bad(f"redo: {n} is not in range 0, {len(top_redos)}")

        redo_linear_idx, redo_idx = top_redos[n]

        redo_of = self.log.get(redo_linear_idx)
        changes = self.log.get(redo_of.prepare_idx).changes

        redo_entry = self.log.get(redo_idx)

        date = now()

        prepare_entry = Operation(
            kind = "prepare-redo",
            description = redo_of.description,
            date = date,

            n = top.n + 1,
            linear_idx = redo_linear_idx,
            prev_idx = top_idx,

            state = redo_entry.state,
            changes = changes,
        )

        prepare_idx = self.log.next_idx()
        self.log.append(prepare_entry)

        commit_entry = Operation(
            kind = "commit-redo",
            description = redo_of.description,
            date = date,

            n = top.n + 1,
            linear_idx = redo_linear_idx,
            prev_idx = top_idx,

            redos = redo_entry.redos,
            state = redo_entry.state,
            prepare_idx = prepare_idx,
        )

        rollback_entry = Operation(
            kind = "rollback-redo",
            description = redo_of.description,
            date = date,

            n = top.n,
            prev_idx = top.prev_idx,
            linear_idx = top.linear_idx,

            redos = top.redos,
            state = top.state,
            prepare_idx = prepare_idx,
        )

        try:
            self.store.apply(changes)
        except:
            self.store.rollback(changes)
            self.log.append(rollback_entry) # rollback
            raise
        else:
            self.log.append(commit_entry)
            # self.linear.append(commit_entry.linear_idx)


    def undo(self):
        top_idx, top = self.log.top()

        if top.linear_idx == 0:
            raise Bad("undo: cannot undo, operation log empty")

        # the operation we're undoing is in the old head's linear_idx
        to_undo = self.log.get(top.linear_idx)

        description = to_undo.description
        changes = self.log.get(to_undo.prepare_idx).changes
        undo_changes = {key: (new, old) for key, (old, new) in changes.items()}

        # the old top's prev_idx is the new head of the operation stack

        old_prev_idx = top.prev_idx
        old_prev = self.log.get(old_prev_idx)

        state = old_prev.state

        # we copy over the old prev's redos, updating
        # the entry for top's linear id, the action
        # we're undoing

        new_redos = []
        for redo_linear_idx, redo_idx in old_prev.redos:
            if redo_linear_idx != top.linear_idx:
                new_redos.append((redo_linear_idx, redo_idx))

        new_redos.append((top.linear_idx, top_idx))

        date = now()

        prepare_entry = Operation(
            kind="prepare-undo",
            description = description,
            date = date,

            n = old_prev.n,
            linear_idx = old_prev.linear_idx,
            prev_idx = old_prev.prev_idx,

            state = old_prev.state,
            changes = undo_changes,
        )

        prepare_idx = self.log.next_idx()
        self.log.append(prepare_entry)

        commit_entry = Operation(
            kind="commit-undo",
            description = description,
            date = date,

            n = old_prev.n,
            linear_idx = old_prev.linear_idx,
            prev_idx = old_prev.prev_idx,

            redos = new_redos,
            state = old_prev.state,
            prepare_idx = prepare_idx,
        )

        rollback_entry = Operation(
            kind = "rollback-undo",
            description = description,
            date = date,

            n = top.n,
            prev_idx = top.prev_idx,
            linear_idx = top.linear_idx,

            redos = top.redos,
            state = top.state,
            prepare_idx = prepare_idx,
        )

        try:
            self.store.apply(undo_changes)
        except:
            self.store.rollback(undo_changes)
            self.log.append(rollback_entry)
            raise
        else:
            self.log.append(commit_entry)

            # o = self.linear.pop(-1)
            # if o != to_undo.linear_idx:
            #    raise Bad(f"undo: internal corruption, popped {o} wanted {to_undo.linear_idx}")

    def print(self):
        # print("linear: ", *self.linear)
        print("store", self.store.d)
        for i, x in enumerate(self.linear_history()):
            print(i, x, sep="\t")
        print()
        for i, x in enumerate(self.log.entries()):
            print(i, x, sep="\t")
        print()


class Log:
    def __init__(self, fh):
        self.fh = fh
        fh.seek(0, 2) # seek to end
        self._next_idx = fh.tell()
        self._top = None

    def next_idx(self):
        return self._next_idx

    def entries(self):
        if self._next_idx == 0:
            return []

        # seek to beginning, decode all files
        out = []
        idx = 0
        self.fh.seek(0)
        while idx < self._next_idx:
            out.append(self._read())
            idx = self.fh.tell()
        return out

    def get(self, idx):
        self.fh.seek(idx)
        return self._read()

    def _read(self):
        header = self.fh.read(81)
        if header[0:9] != b"json+len=" or header[-1:] != b'\n':
            raise Bad("corrupt file: header", header)

        length = int(header[10:26], 16)
        body = self.fh.read(length)

        footer = self.fh.read(82)
        if footer[0:10] != b"\njson-len=" or footer[-1:] != b'\n':
            raise Bad("corrupt file: footer", footer)

        footer_length = int(footer[11:27], 16)

        if length != footer_length:
            raise Bad("corrupt file")

        return Operation(**json.loads(body))

    def top(self):
        self.fh.seek(-82, 2)

        footer = self.fh.read(82)
        if footer[0:10] != b"\njson-len=" or footer[-1:] != b'\n':
            raise Bad("corrupt file: footer")

        footer_length = int(footer[11:27], 16)

        idx = self.fh.tell() -81 - 81 - footer_length -1

        return idx, self.get(idx)

    def append(self, op):
        self.fh.seek(0, 2)

        body = json.dumps(vars(op), indent=8).encode('utf-8')
        length = len(body)

        pad = " "*(80-9-16)

        self.fh.write(f"json+len={length:016x}{pad}\n".encode('utf-8'))
        self.fh.write(body)
        self.fh.write(f"\njson-len={length:016x}{pad}\n".encode('utf-8'))

        self._next_idx = self.fh.tell()




class Store:
    def __init__(self, fh):
        self.fh = fh
        self.d = {}

    def load(self):
        self.fh.seek(0)
        self.d = json.loads(self.fh.read().decode('utf-8'))

    def write(self):
        self.fh.seek(0)
        self.fh.write(json.dumps(self.d, indent=8).encode('utf-8'))
        self.fh.truncate()

    def set(self, k, v):
        self.d[k] = v

    def get(self, k):
        return self.d.get(k)

    def apply(self, changes):
        for k in changes:
            old, new = changes[k]
            if self.d.get(k) != old:
                raise Bad("oh no: store out of sync")
            self.d[k] = new
        self.write()

    def rollback(self, changes):
        for k in changes:
            old, new = changes[k]
            current = self.d.get(k)
            if current == new:
                self.d[k] = old
            elif current != old:
                raise Bad("oh no")
        self.write()


def more_example_code():
    l = OpLog(FakeLog("test"), FakeStore("test"))
    l.init({})

    with l.do("A") as txn:
        txn.set_state("foo", "A")

    with l.do("B") as txn:
        txn.set_state("bar", "B")

    with l.do("C") as txn:
        txn.set_state("foo", "C")

    with l.do("D") as txn:
        txn.set_state("bar", "D")

    with l.do("E") as txn:
        txn.set_state("foo", "E")
        txn.set_state("bar", "E")

    for _ in (1,2):
        for x in range(1, 6):
            for _ in range(x):
                l.undo()

            for _ in range(x):
                l.redo()

    for _ in range(5):
        l.undo()
        l.redo()

    with l.do("f6") as txn:
        txn.set_state("f6", True)
    l.undo()

    l.print()
    l.compact(FakeLog("test"))
    l.print()

def still_more_example_code():
    l = OpLog(FakeLog("test"), FakeStore("test"))
    l.init({"internal": 0})

    with l.do("0") as txn:
        txn.set_state("internal", "run")

    for x in range(4):
        with l.do("*"+str(x)) as txn:
            txn.set_state( "internal", "foo")
        with l.do("+"+str(x)):
            txn.set_state("internal", "bar")
        l.undo()
        l.undo()

    for _ in range(4):
        l.redo(0)
        l.redo()
        l.undo()
        l.undo()


    l.undo()
    l.redo()
    l.undo()
    l.redo()

    l.print()
    l.compact(FakeLog("test"))
    l.print()

def run_all_examples():
        print("---")
        example_code()
        print("---")
        more_example_code()
        print("---")
        still_more_example_code()
        print()


if __name__ == '__main__':
    import sys
    import os

    commands = {
        "example": "        # run example code in memory",
        "create": "         # create a log and store file ",
        "set": "key=value   # set key to value in store",
        "get": "key         # get key from store",
        "undo": "           # undo last action",
        "redo": "<n>        # redo last undo action",
        "redos": "          # list all redoable actions from current action",
        "changes": "        # list all changes to the store",
        "history": "        # list all operations, including undo/redo",
        "compact": "        # remove all undo/redo operations from history, cannot be undone",
        "help": "           # this text",
    }

    if len(sys.argv) >= 2:
        arg = sys.argv[1]
    else:
        arg = "help"
    if arg not in commands:
        arg = "help"

    if arg == "help":
        for k, v in commands.items():
            print(sys.argv[0], k, v)
        print()
        sys.exit(-1)
    elif arg == "example":
        run_all_examples()
        sys.exit(-1)
    elif arg == "create":
        # if exists, exit

        with open("log", "xb+") as log_fh, open("store", "xb+") as store_fh:
            log = Log(log_fh)
            store = Store(store_fh)
            store.write()

            oplog = OpLog(log, store)
            oplog.init({"file":"store"})

        sys.exit(0)
    elif arg == "compact":
        with open("log", "ab+") as log_fh, open("new_log", "xb+") as new_log_fh:
            log = Log(log_fh)
            top_idx, top = log.top()
            store_file = top.state["file"]

            with open(store_file, "rb+") as store_fh:
                store = Store(store_fh)
                store.load()
                oplog = OpLog(log, store)
                oplog.recover()

                new_log = Log(new_log_fh)

                oplog.compact(new_log)

        os.replace("new_log", "log")

    with open("log", "ab+") as log_fh:
        log = Log(log_fh)

        top_idx, top = log.top()
        store_file = top.state["file"]

        with open(store_file, "rb+") as store_fh:
            store = Store(store_fh)
            store.load()
            oplog = OpLog(log, store)
            oplog.recover()

            if arg == "get":
                for name in sys.argv[2:]:
                    value = store.get(name)
                    if value is not None:
                        print(f"{name}:{store.get(name)}")
            elif arg == "changes":
                for line in oplog.linear_history():
                    print(line)
            elif arg == "history":
                for line in oplog.history():
                    print(line)
            elif arg == "redos":
                for i, r in enumerate(oplog.redos()):
                    print(i, r)

            else:
                if top.kind == "commit-close":
                    print("error: log cannot be edited")

                elif arg == "set":
                    with oplog.do(" ".join(sys.argv[1:])) as txn:
                        for arg in sys.argv[2:]:
                            key, value = arg.split('=')
                            if value == "":
                                  value = None
                            print(f"{key}: {value}")
                            txn.set_store(key, value)
                elif arg == "undo":
                    oplog.undo()
                    print("undo")
                elif arg == "redo":
                    n = -1
                    if len(sys.argv[2:]) >= 1:
                        n = int(sys.argv[2])
                    oplog.redo(n)
                    print("redo")
    print()
    sys.exit(-1)




