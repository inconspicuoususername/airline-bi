#!/usr/bin/env python3

"""
Overcomplicated pipeline for parallelized data generation.
This is mainly used for synthesizing data for the operational database in a timely manner.
Multiprocessing is used as opposed to threading to avoid GIL issues.
"""

from typing_extensions import ParamSpec
import multiprocessing as mp
from multiprocessing.sharedctypes import Synchronized
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from model.reldb import Base, Customer
from faker import Faker
from tqdm import tqdm
import random
import time
import os
from typing import Callable, Concatenate, TypeVar

# Config
# DB_URL = "postgresql+psycopg2://davud:password@localhost/airline_db"

try:
    mp.set_start_method("spawn")  #MacOS and Windows
except RuntimeError:
    pass

class PipelineConfig:
    def __init__(self, 
                 num_rows: int,
                 producer_extra_args: tuple = (),
                 batch_size: int = 5000, 
                 num_producers: int = 8, #cca num of cores, this should be set to the number of cores on the machine but i cba rn
                 num_consumers: int = 3, 
                 queue_maxsize: int = 16, 
                 max_retries: int = 3,
                 ):
        self.num_rows = num_rows
        self.producer_extra_args = producer_extra_args
        self.batch_size = batch_size
        self.num_producers = num_producers
        self.num_consumers = num_consumers
        self.max_retries = max_retries
        self.queue_maxsize = queue_maxsize

def default_session_factory():
    DB_URL = "postgresql+psycopg2://davud:password@localhost/airline_db"
    engine = create_engine(DB_URL)
    Session = sessionmaker(bind=engine)
    return Session()

T = TypeVar('T', bound=Base)
type ProducerFn[T: Base, **Param] = Callable[Concatenate[Faker, int, int, Param], tuple[list[T], ...]]


#pray for cache coherency
def SAMPLE_PRODUCER_FN(fake: Faker, batch_offset: int, batch_size: int, *args) -> tuple[list[Customer], ...]:
    return ([
        Customer(
            full_name=fake.name(),
            email=fake.email(),
            frequent_flyer=random.choice(['Yes', 'No'])
        )
        for _ in range(batch_size)
    ],)

P = ParamSpec('P')

def producer_wrapper(queue: mp.JoinableQueue,
                     process_index: int,
                     num_batches: int, 
                     batch_size: int, 
                     producer_fn: ProducerFn[T, P],
                     *producer_extra_args: P.args,
                     **producer_extra_kwargs: P.kwargs,
                     ):
    fake = Faker()
    for batch_num in range(num_batches):
        batch = producer_fn(fake, process_index * num_batches + batch_num, batch_size, *producer_extra_args, **producer_extra_kwargs)
        queue.put(batch)

def consumer_wrapper(queue: mp.JoinableQueue, 
                     counter: Synchronized, 
                     max_retries: int, 
                     consumer_session_factory: Callable[[], Session],
                     ):
    session = consumer_session_factory()
    while True:
        batch = queue.get()
        if batch is None:
            queue.task_done()
            break
        for attempt in range(max_retries):
            try:
                for list_of_objs in batch:
                    session.bulk_save_objects(list_of_objs)
                session.commit()
                with counter.get_lock():
                    counter.value += 1
                break
            except Exception as e:
                print(f"[PID {os.getpid()}] Insert failed: {e}. Retrying ({attempt+1})...")
                time.sleep(0.5)
        queue.task_done()
    session.close()

def run_pipeline(
        config: PipelineConfig,
        producer_fn: ProducerFn[T, P],
        consumer_session_factory: Callable,
):
    total_batches = config.num_rows // config.batch_size
    queue = mp.JoinableQueue(maxsize=config.queue_maxsize)
    counter = mp.Value('i', 0) #progress c ounter
    producers = []
    batches_per_producer = total_batches // config.num_producers
    remainder = total_batches % config.num_producers
    for process_index in range(config.num_producers):
        _batches = batches_per_producer
        if remainder > 0:
            _batches += 1
            remainder -= 1
        p = mp.Process(target=producer_wrapper, args=(queue, process_index, _batches, config.batch_size, producer_fn, *config.producer_extra_args))
        p.start()
        producers.append(p)
    consumers = []
    for _ in range(config.num_consumers):
        c = mp.Process(target=consumer_wrapper, args=(queue, counter, config.max_retries, consumer_session_factory))
        c.start()
        consumers.append(c)

    # Progress monitoring
    counter_bar = tqdm(total=total_batches, desc="Inserting Batches", ncols=80, position=0)
    while True:
        with counter.get_lock():
            counter_bar.n = counter.value
            counter_bar.refresh()
            if counter.value >= total_batches:
                break
        time.sleep(0.2)
        # queue_bar.n = queue.full
        # queue_bar.refresh()

    # queue_bar.close()
    counter_bar.close()

    # # Wait for all tasks to be marked as done
    # queue.join()

    # Clean up
    for p in producers:
        p.join()

    for _ in range(config.num_consumers):
        # Signal consumers to wrap up
        queue.put(None)

    for c in consumers:
        c.join()

    queue.close()

    print("+++++ All data inserted.")

if __name__ == "__main__":
    # run_pipeline(
    #     producer_fn=SAMPLE_PRODUCER_FN,
    #     consumer_fn=SAMPLE_CONSUMER_FN,
    # )
    run_pipeline(
        config=PipelineConfig(
            num_rows=100_000,
        ),
        consumer_session_factory=default_session_factory,
        producer_fn=SAMPLE_PRODUCER_FN,
        # consumer_fn=SAMPLE_CONSUMER_FN,
    )
