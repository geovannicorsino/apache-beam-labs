"""
ReadFromText | Read lines from a local text file into a PCollection.

Each line of the file becomes one element in the PCollection. TakeFirstN is a
stateful DoFn that limits the output to the first N elements — useful for
previewing large files without loading everything into memory.

Note: TakeFirstN works correctly on the DirectRunner but may produce inconsistent
results on distributed runners (Dataflow), since each worker maintains its own counter.

Example input (data/dept_data.txt):
    emp_id,name,department
    1,Alice,HR
    2,Bob,Engineering
    ...
Example output (first 10 lines):
    emp_id,name,department
    1,Alice,HR
    2,Bob,Engineering
"""
import apache_beam as beam


class TakeFirstN(beam.DoFn):
    def __init__(self, n):
        self.n = n
        self.count = 0

    def process(self, element):
        if self.count < self.n:
            self.count += 1
            yield element


if __name__ == '__main__':
    with beam.Pipeline() as p:
        names = (
            p
            | "Read from Text File" >> beam.io.ReadFromText("./data/dept_data.txt")
            | "Take first 10 lines" >> beam.ParDo(TakeFirstN(10))
            | "Print output" >> beam.Map(print)
        )
