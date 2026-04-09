# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep

class UserTagCounter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_user_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_user_tags(self, _, line):
        if line.startswith("userId"):
            return
        try:
            fields = line.strip().split(',')
            if len(fields) >= 1:
                userID = fields[0]
                yield userID, 1
        except Exception:
            pass  # Ligne ignorée si erreur

    def reducer_count_tags(self, userID, counts):
        yield userID, sum(counts)

if __name__ == '__main__':
    UserTagCounter.run()
