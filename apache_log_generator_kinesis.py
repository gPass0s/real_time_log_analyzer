#!/usr/bin/python3
# -*- encoding: utf-8 -*-
"""
Created on Mon Jul 20 12:19 BRT 2020
author: guilherme passos | twitter: @gpass0s
"""

import datetime
import time
import numpy
import socket
import random
from tzlocal import get_localzone

from faker import Faker

faker = Faker()

timestr = time.strftime("%Y%m%d-%H%M%S")
inital_time = datetime.datetime.now()

responses = ["200", "404", "500", "301"]
methods = ["GET", "POST", "DELETE", "PUT"]
resources = [
    "/list",
    "/wp-content",
    "/wp-admin",
    "/explore",
    "/search/tag/list",
    "/app/main/posts",
    "/posts/posts/explore",
    "/apps/cart.jsp?appID=",
]
ualist = [
    faker.firefox,
    faker.chrome,
    faker.safari,
    faker.internet_explorer,
    faker.opera,
]
local = get_localzone()

while True:

    increment = datetime.timedelta(seconds=random.randint(30, 300))
    inital_time += increment

    ip_address = faker.ipv4()
    data_time = inital_time.strftime("%d/%b/%Y:%H:%M:%S")
    time_zone = datetime.datetime.now(local).strftime("%z")
    method = numpy.random.choice(methods, p=[0.6, 0.1, 0.1, 0.2])
    endpoint = random.choice(resources)
    if "apps" in endpoint:
        endpoint += str(random.randint(1, 5))
    response = numpy.random.choice(responses, p=[0.85, 0.09, 0.02, 0.04])
    content_size = int(random.gauss(5000, 50))
    referer = faker.uri()
    user_agent = numpy.random.choice(ualist, p=[0.5, 0.3, 0.1, 0.05, 0.05])()

    timestr = inital_time.strftime("%Y%m%d-%H%M%S")
    outFileName = f"/var/log/log_analyzer/access_log_{timestr}.log"
    f = open(outFileName,'w')

    log = '{} - - [{} {}] "{} {} HTTP/1.0" {} {} "{}" "{}"\n'.format(
        ip_address,
        data_time,
        time_zone,
        method,
        endpoint,
        response,
        content_size,
        referer,
        user_agent,
    )

    f.write(log)
    f.flush()
    f.close()
    time.sleep(random.uniform(0.01,1))
