class ReplicatedLog:
    """The list of accepted jobs that every manager keeps a copy of.

    Entries are numbered 1, 2, 3 ... Each entry remembers the term
    (election round) in which the leader wrote it.
    """

    def __init__(self, node_id, total_nodes):
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.entries = []          # [{"term": 1, "command": {...}}, ...]
        self.commit_index = 0      # entries 1..commit_index are safe
        self._acks = {}            # entry number -> set of node ids that have it

    def _majority(self):
        return self.total_nodes // 2 + 1

    def last_index(self):
        return len(self.entries)

    def term_at(self, index):
        return self.entries[index - 1]["term"] if index >= 1 else 0

    # ---- leader side -----------------------------------------------------
    def append(self, term, command):
        """Leader writes a new entry. Returns its number."""
        self.entries.append({"term": term, "command": command})
        index = len(self.entries)
        self._acks[index] = {self.node_id}      # the leader has it
        self._advance_commit()
        return index

    def record_ack(self, index, node_id):
        """A follower says it has entries up to `index`."""
        for i in range(self.commit_index + 1, index + 1):
            self._acks.setdefault(i, set()).add(node_id)
        self._advance_commit()
        return self.commit_index >= index

    def entries_from(self, index):
        """What to send a follower that already has entries 1..index-1."""
        return self.entries[index - 1:]

    def _advance_commit(self):
        while self.commit_index < len(self.entries):
            nxt = self.commit_index + 1
            if len(self._acks.get(nxt, ())) >= self._majority():
                self.commit_index = nxt
            else:
                break

    # ---- follower side ---------------------------------------------------
    def accept(self, prev_index, prev_term, entries):
        """Follower: add entries after prev_index. False = I'm missing some."""
        if prev_index > len(self.entries):
            return False
        if prev_index >= 1 and self.term_at(prev_index) != prev_term:
            return False
        self.entries = self.entries[:prev_index] + list(entries)
        return True

    def committed(self):
        return self.entries[:self.commit_index]