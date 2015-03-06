# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


from ryu import cfg
from ryu.base import app_manager
from ryu.lib import hub
from ryu.lib.xflow import sflow
from ryu.lib.packet.ethernet import ethernet
from ryu.lib.packet.ipv4 import ipv4

opts = [cfg.StrOpt('address', default='0.0.0.0',
                   help='sFlow Collector bind address'),
        cfg.IntOpt('port', default=6343,
                   help='sFlow Collector port'),
        cfg.IntOpt('max_udp_msg_size', default=1472,
                   help='Maximum size of UDP messages')]

cfg.CONF.register_opts(opts, 'plow')


class SFlow(app_manager.RyuApp):
    def __init__(self, *args, **kwargs):
        super(SFlow, self).__init__(*args, **kwargs)
        self._address = self.CONF.plow.address
        self._port = self.CONF.plow.port
        self._udp_msg_size = self.CONF.plow.max_udp_msg_size
        self._udp_sock = None

    def _handle(self, buf, addr):
        packet = sflow.sFlow.parser(buf)

        if not packet:
            return
        # sFlowV5RawPacketHeader
        flow_data = None

        for sFlow_v5_samples in packet.samples:
            if sFlow_v5_samples is not None:
                sFlow_v5_sample = sFlow_v5_samples.sample
                if type(sFlow_v5_sample) is sflow.sFlowV5FlowSample:
                    for flow_record in sFlow_v5_sample.flow_records:
                        if flow_record.flow_data_format == 1:
                            fl_data = flow_record.flow_data
                            if type(fl_data) is sflow.sFlowV5RawPacketHeader:
                                flow_data = flow_record.flow_data

        header_str = ''
        if flow_data is not None:
            for key in flow_data.header:
                header_str += key
            res, ptype, e_buf = ethernet.parser(header_str)
            print("src: %s" % res.src)
            print("dst: %s" % res.dst)

            ipv4res, ipv4ptype, _ = ipv4.parser(e_buf)
            print("version: %s" % ipv4res.version)
            print("header length: %s" % ipv4res.header_length)
            print("tos: %s" % ipv4res.tos)
            print("total length: %s" % ipv4res.total_length)
            print("identification: %s" % ipv4res.identification)
            print("flags: %s" % ipv4res.flags)
            print("offset: %s" % ipv4res.offset)
            print("ttl: %s" % ipv4res.ttl)
            print("proto: %s" % ipv4res.proto)
            print("csum: %s" % ipv4res.csum)
            print("src: %s" % ipv4res.src)
            print("dest: %s" % ipv4res.dst)
            print("proto type: %s" % ptype)

        print flow_data



        print packet.__dict__

    def _recv_loop(self):
        self.logger.info('Listening on %s:%s for sflow agents' %
                         (self._address, self._port))

        while True:
            buf, addr = self._udp_sock.recvfrom(self._udp_msg_size)
            t = hub.spawn(self._handle, buf, addr)
            self.threads.append(t)

    def start(self):
        self._udp_sock = hub.socket.socket(hub.socket.AF_INET,
                                           hub.socket.SOCK_DGRAM)
        self._udp_sock.bind((self._address, self._port))

        t = hub.spawn(self._recv_loop)
        super(SFlow, self).start()
        return t
