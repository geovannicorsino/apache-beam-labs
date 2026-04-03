import apache_beam as beam

p1 = beam.Pipeline()


names = (
    p1
    | "Create a list of names" >> beam.Create(["Alice", "Bob", "Charlie"])
    | "Write to Text File" >> beam.io.WriteToText("./data/test/text", num_shards=1)
)

p1.run().wait_until_finish()
