import time
from kafka import KafkaConsumer, TopicPartition

BOOTSTRAP = "localhost:9092"
TOPICS = ["binance-raw", "coinbase-raw"]
SAMPLE_SECONDS = 1
DURATION_SECONDS = 300  # measure for 5 minutes

peak_topic_rates = {topic: 0 for topic in TOPICS}
topic_rate_history = {topic: [] for topic in TOPICS}

consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP,
    enable_auto_commit=False
)

def get_total_end_offset(topic):
    partitions = consumer.partitions_for_topic(topic)
    while not partitions:
        time.sleep(0.5)
        partitions = consumer.partitions_for_topic(topic)
        
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    end_offsets = consumer.end_offsets(topic_partitions)

    return sum(end_offsets.values())

previous = {topic: get_total_end_offset(topic) for topic in TOPICS}

peak_rate = 0
rates = []

print("Measuring Kafka ingestion rate...")

for _ in range(DURATION_SECONDS // SAMPLE_SECONDS):
    time.sleep(SAMPLE_SECONDS)

    current = {topic: get_total_end_offset(topic) for topic in TOPICS}

    total_new_events = sum(current[t] - previous[t] for t in TOPICS)
    rate = total_new_events / SAMPLE_SECONDS

    topic_rates = {
        topic: (current[topic] - previous[topic]) / SAMPLE_SECONDS
        for topic in TOPICS
    }

    for topic, topic_rate in topic_rates.items():
        peak_topic_rates[topic] = max(peak_topic_rates[topic], topic_rate)
    for topic, topic_rate in topic_rates.items():
        topic_rate_history[topic].append(topic_rate)

    rates.append(rate)
    peak_rate = max(peak_rate, rate)

    print(f"\nTotal rate: {rate:.2f} events/sec")

    for topic, topic_rate in topic_rates.items():
        print(f"{topic}: {topic_rate:.2f} events/sec")

    previous = current

avg_rate = sum(rates) / len(rates) if rates else 0

print("\nResults")
print(f"Average ingestion rate: {avg_rate:.2f} events/sec")
print(f"Peak ingestion rate: {peak_rate:.2f} events/sec")

print("\nPeak rate by topic")
for topic, peak in peak_topic_rates.items():
    print(f"{topic}: {peak:.2f} events/sec")

print("\nAverage rate by topic")
for topic, history in topic_rate_history.items():
    avg = sum(history) / len(history) if history else 0
    print(f"{topic}: {avg:.2f} events/sec")