# A write-ahead-log with undo and redo

```

./undo.py create

./undo.py set a=1 

./undo.py undo

./undo.py redo

```

# linear history 

undo and redo can be implemented atop list of actions, where
each new action adds onto the list. undo removes the top of the list
and updates the predecessor to point to a redo for the old top. a
redo reverses the process, restoring an item to the top of the list

for example:

> do A, do B, do C, do D, Do E

when we undo E, D becomes the head of the list,
and now has a redo for E inside:

> do A, do B, do C, do D (redo=do E)

redoing does the opposite, taking a list item out of
the redo list, and adding it back to the history

> do A, do B, do C, do D, do E

this is the "linear history" representation of undo and
redo.

# persistent history

persisting the linear history isn't easy. persisting
a mutable structure means writing the whole thing to
disk each time,

(that, or using something like sqlite)

ideally, we'd like to store the undo/redo history
atop of something like a log, where we can cheaply
append new operations, and quickly read in the last
entry, rather than having to read the entire thing
into memory each time.

# an undo/redo log

instead of looking at the linear history, we'll look
at the operation history:

> do A, do B, do C, do D, do E, undo E, redo E

unlike the "linear history", the log doesn't require
any mutation, or update in place to function. this
means we can write these operations to disk with
ease.

# reconstructing the linear history

we'd still like to see and use the linear history,
so we annotate each operation in the log with enough
information to reconstruct it.

1. do A (do 1, prev 0)
2. do B (do 2, prev 1)
3. do C (do 3, prev 2)
4. do D (do 4, prev 3)
5  do E (do 5, prev 4)
6. undo E (do 4, prev 3) -- we're pretending to be `do D` at the top of the list
7. redo E (do 5, prev 6) -- we're pretending to be `do E` at the top, and we point to the undo behind us

each record carries a `do` and `prev` id, which explains which `do` was last applied,
and the predecessor. this is enough to reconstruct the linear history, really.

we store redo options as (original do id, last redo) so that we can work out
if an undid action already has a redo, and to update it.

# recovery

undo and redo are good, but they aren't enough: we can make things more
robust by using a pair of `prepare` `commit` entries. one to write the
changes we plan too make, another to mark success.


1. prepare-do A
2. commit-do A
3. prepare-undo A
4. rollback-undo A

if we load a log with just a `prepare`, we can unwind it when we load,
much like applying an undo or redo. splitting the records in two has
another benefit: it lets us store commit metadata (date, description)
and commit data (raw changes) in different places, so loading one
doesn't require loading the other.

this makes reading history a lot faster.

# running the code

```
./undo.py create

./undo.py set a=1 b=2

./undo.py get a=1

./undo.py undo

./undo.py redos # list redos

./undo.py redo 0

./undo.py changes # list changes without undo/redo

./undo.py history # list all operations

./undo.py compact # remove all undos/redos from history (permanently)
```

# example code

The `OpLog` takes a `Log` and a `Store`. The undo log tracks changes
to the store in form of (old, new) pairs, and stores a `state` dict inside
the operation headers directly.


```
log = OpLog(FakeLog("test"), FakeStore("test"))

l.init({"internal": False}) # a log has a state field
```

Operations are in the form of transactions:

```
with log.do("A") as txn:
    txn.set_store("foo", "A") # we can also make changes to store

with log.do("B") as txn:
    txn.set_store("foo", "B") # changes applied to FakeStore
    txn.set_store("bar", "E")

    txn.set_state("internal", True) ## stored inside the log
```

Undo and redo are normal function calls:

```
l.undo()
for r in l.redos():
    print("redo", r)
l.redo()
```




