"""
a write-ahead-log with undo and redo

undo and redo can be implemented atop list of actions, where
each new action adds onto the list, and undo removes it from
the list, and updates the predecessor with the new redo option,
and redo does the opposite process

say we have some history:

> do A, do B, do C, do D, Do E

when we undo E, D becomes the head of the list,
and now has a redo for E inside:

> do A, do B, do C, do D (redo=do E)

redoing does the opposite, taking a list item out of
the redo list, and adding it back to the history

> do A, do B, do C, do D, do E

this is the "linear history" representation of undo and
redo.

persisting the linear history as a mutable structure
means writing the whole thing to disk each time,
or using something like sqlite to handle updates
in a more piecemeal style.

ideally, we'd like to store the undo/redo history
atop of something like a log, where we can cheaply
append new operations, and quickly read in the last
entry.

instead, we'll use something else, and then
adapt it to get back something that looks
and feels like the linear history: an operation log

> do A, do B, do C, do D, do E, undo E, redo E

unlike the "linear history", the log doesn't require
any mutation, or update in place to function. this
means we can write these operations to disk with
ease.

we'd still like to see and use the linear history,
so we annotate each operation in the log with enough
information to reconstruct it.

we need a "this is the original do" pointer, and also
"this is the predecessor" pointer, and thats it

1. do A (do 1, prev 0)
2. do B (do 2, prev 1)
3. do C (do 3, prev 2)
4. do D (do 4, prev 3)
5  do E (do 5, prev 4)
6. undo E (do 4, prev 3) -- we're pretending to be `do D` at the top of the list
7. redo E (do 5, prev 6) -- we're pretending to be `do E` at the top, and we point to the undo behind us

to turn this from an operations log into a write ahead
log, we split each do/redo/undo into two entries, a prepare
and a commit (or rollback).

1. prepare-do A
2. commit-do A
3. prepare-undo A
4. rollback-undo A

this split has an additional benefit: we can put
the operation details inside the prepare entry,
and omit them from the commit entry entirely.

this means that scanning through the history
doesn't require loading all the operations
from disc, just the commit data headers
"""

def example_code():
    l = OpLog(FakeLog("test"), FakeStore("test"), {"internal": False})

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
    l.redo()

    l.print()

    l.compact()
    l.print()

# ----

from contextlib import contextmanager
from datetime import datetime, timezone

def now():
    return datetime.now(timezone.utc)

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

    def new(self, name):
        return FakeLog(name)


class FakeStore:
    def __init__(self, name):
        self.d = {}
        self.name = name

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
    def __init__(self, n, kind, description, prev_idx=None, linear_idx=None, redos=(), state=None, changes=None, prepare=None, date=None):
        self.kind = kind                # commit or prepare, for do, undo or redo
        self.description = description  # the description
        self.date = date                # date of operation

        self.n = n                      # n is the operation number in the linear history
        self.linear_idx = linear_idx    # the commit that was originally run to get here
        self.prev_idx = prev_idx        # the previous operation in the linear history

        self.state = state              # some state of the world, mutated by actions
        self.redos = redos              # a (linear_idx, last_redo_idx) list of operations to redo

        self.changes = changes          # in a prepare operation, this contains the changes to the store
        self.prepare = prepare          # in a commit operation, this points to the prepare

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
    def __init__(self, log, store, state):
        self.log = log
        self.store = store
        self.linear = [] # used for correctness checking

        if self.log.next_idx() == 0:
            init = Operation(n=0, kind="commit-init", description="", linear_idx=0, state=state, date=now())
            self.log.append(init)

    def state(self):
        top_idx, top = self.top()
        return top.state

    def full_history(self):
        return [f"{x.n} {x.kind}: {x.description}, {x.state}" for x in self.log.entries()]

    def linear_history(self):
        top_idx, top = self.log.top()
        if top_idx == 0 or top.linear_idx == 0:
            return []

        out = []
        while top.linear_idx > 0:
            linear_idx = self.log.get(top.linear_idx)

            action = f"{top.n} {linear_idx.kind}: {linear_idx.description}, {top.state}"
            out.append(action)

            top = self.log.get(top.prev_idx)

        out.reverse()
        return out

    def recover(self):
        top_idx, top = self.log.top()

        if top.kind.startswith("commit-"):
            return

        prev_idx = top.prev
        prev = self.log.get(prev_idx)

        date = now()

        rollback_entry = Operation(
            kind = top.kind.replace("prepare-rollback-"),
            description = top.description,
            date = date,

            n = prev.n,
            prev_idx = prev.prev_idx,
            linear_idx = prev.linear_idx,

            redos = prev.redos,
            state = prev.state,
            prepare = top_idx,
        )

        changes = top.changes
        self.store.rollback(changes)
        self.log.commit(rollback_entry)


    def compact(self):
        top_idx, top = self.log.top()
        new_log = self.log.new(self.log.name)

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

            prepare = self.log.get(linear_top.prepare)

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
                prepare = prepare_idx,
            )
            new_log.append(commit_entry)

            prev_idx = linear_idx

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
            prepare = prepare_idx,
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
            prepare = prepare_idx,
        )

        try:
            self.store.apply(changes)
        except:
            self.store.rollback(changes)
            self.log.append(rollback_entry)
            raise
        else:
            self.log.append(commit_entry)
            self.linear.append(commit_entry.linear_idx)


    def redo(self, n=-1):
        top_idx, top = self.log.top()
        top_redos = top.redos

        if len(top_redos) == 0:
            raise  Bad("redo: othing to redo, empty operation log")
        elif n < -len(top_redos)  or n >= len(top_redos):
            raise Bad(f"redo: {n} is not in range 0, {len(top_redos)}")

        redo_linear_idx, redo_idx = top_redos[n]

        redo_of = self.log.get(redo_linear_idx)
        changes = self.log.get(redo_of.prepare).changes

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
            prepare = prepare_idx,
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
            prepare = prepare_idx,
        )

        try:
            self.store.apply(changes)
        except:
            self.store.rollback(changes)
            self.log.append(rollback_entry) # rollback
            raise
        else:
            self.log.append(commit_entry)
            self.linear.append(commit_entry.linear_idx)


    def undo(self):
        top_idx, top = self.log.top()

        if top.linear_idx == 0:
            raise Bad("undo: cannot undo, operation log empty")

        # the operation we're undoing is in the old head's linear_idx
        to_undo = self.log.get(top.linear_idx)

        description = to_undo.description
        changes = self.log.get(to_undo.prepare).changes
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
            prepare = prepare_idx,
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
            prepare = prepare_idx,
        )

        try:
            self.store.apply(undo_changes)
        except:
            self.store.rollback(undo_changes)
            self.log.append(rollback_entry)
            raise
        else:
            self.log.append(commit_entry)

            o = self.linear.pop(-1)
            if o != to_undo.linear_idx:
                raise Bad(f"undo: internal corruption, popped {o} wanted {to_undo.linear_idx}")

    def print(self):
        print("linear: ", *self.linear)
        print("store", self.store.d)
        for i, x in enumerate(self.linear_history()):
            print(i, x, sep="\t")
        print()
        for i, x in enumerate(self.log.entries()):
            print(i, x, sep="\t")
        print()


class Logfile:
    def __init__(self, name):
        self.i = []
        self.name = name

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

    def new(self, name):
        return FileLog(name)


class Storefile:
    def __init__(self):
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


def more_example_code():
    l = OpLog(FakeLog("test"), FakeStore("test"), {})

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
    l.compact()
    l.print()

def still_more_example_code():
    l = OpLog(FakeLog("test"), FakeStore("test"), {"internal": 0})

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
    l.compact()
    l.print()

if __name__ == '__main__':
    import sys

    if len(sys.argv) >= 2:
        arg = sys.argv[1]
    else:
        arg = "help"

    if arg == "example":
        print("---")
        example_code()
        print("---")
        more_example_code()
        print("---")
        still_more_example_code()
        print()
    else:
        print(sys.argv[0], "example")
        print()
        sys.exit(-1)

