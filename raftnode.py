import rpyc
import sys
import time
import random
import _thread
import threading
import logging

'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''

FOLLOWER_TIMER_RANGE = (1, 1.5)
CANDIDATE_TIMER_RANGE = (1, 1.5)
HEARTBEAT_TIMER_RANGE = (0.2, 0.4)

def get_random_from_range(low, high):
    return random.random() * (high-low) + low

class Event:
    def __init__(self, type):
        self.type = type
        self.attrs = dict()


class RaftNode(rpyc.Service):
    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """

    def __init__(self, config, node_id):
        with open(config, 'r') as f:
            lines = f.readlines()

        self.no_of_nodes = int(lines[0].split(':')[-1])

        self.node_port = list()
        for i in range(self.no_of_nodes):
            this_port = int(lines[i + 1].split(':')[-1])
            self.node_port.append(this_port)
            logging.info("Port"+str(i)+": "+str(this_port))

        self.node_id = node_id
        self.cur_term = 0
        self.cur_leader = -1
        self.vote_for = -1

        self.switch_to_follower()

    def exposed_request_vote(self, term, candidate_id):
        logging.info("Received RPC call to request_vote. Term:"+str(term)+", candidate_id: "+str(candidate_id))
        if term > self.cur_term:
            logging.info("Term is larger than local cur_term, vote granted.")
            self.cur_term = term
            self.vote_for = candidate_id
            self.switch_to_follower()
            return term, True
        if term < self.cur_term:
            logging.info("Term is smaller than local cur_term, vote not granted.")
            return self.cur_term, False

        if self.vote_for != -1:
            logging.info("Term is same as local cur_term, vote not granted (already voted).")
            return self.cur_term, False
        else:
            logging.info("Term is same as local cur_term, vote  granted.")
            self.vote_for = candidate_id
            return self.cur_term, True

    def exposed_append_entries(self, term, leader_id):
        logging.info("Received RPC call to append entries. Term:"+str(term)+", leader_id:"+str(leader_id))

        if term >= self.cur_term:
            logging.info("Find node which has larger (or equal) term:"+str(term)+", while local cur_term:"+str(self.cur_term))
            self.cur_term = term
            self.cur_leader = leader_id
            self.switch_to_follower()
            logging.info("Switch to follower...")
            return term, True

        self.reset_follower_timer()
        if term < self.cur_term:
            return self.cur_term, False
        # term == self.cur_term
        return term, True

    def switch_to_follower(self):
        logging.info("Become follower.")
        if hasattr(self, "follower_timer"):
            self.follower_timer.cancel()
            logging.info("Reset follower_timer(in switch_to_follower).")
        follower_timer_interval = get_random_from_range(FOLLOWER_TIMER_RANGE[0],FOLLOWER_TIMER_RANGE[1])
        self.follower_timer = threading.Timer(follower_timer_interval, self.switch_to_candidate)
        self.follower_timer.start()
        logging.info("Follower timer starts. Interval:" + str(follower_timer_interval)[0:5] + " seconds.")
        
    def switch_to_candidate(self):
        logging.info("Switch to candidate.")
        self.follower_timer.cancel()
        self.start_election()

        candidate_timer_interval = get_random_from_range(CANDIDATE_TIMER_RANGE[0], CANDIDATE_TIMER_RANGE[1])
        logging.info("Candidate timer interval:"+str(candidate_timer_interval)[0:5])
        self.candidate_timer = threading.Timer(candidate_timer_interval, self.switch_to_candidate)
        self.candidate_timer.start()

    def switch_to_leader(self):
        logging.info("Switch to leader.")
        self.candidate_timer.cancel()
        self.cur_leader = node_id
        self.broadcast_heartbeat_periodically()

    def leader_step_down(self, new_leader):
        logging.info("Leader step down.")
        self.vote_for = -1
        switch_to_follower()

    def reset_follower_timer(self):
        self.follower_timer.cancel()
        logging.info("Reset follower_timer(in reset_follower_timer).")
        follower_timer_interval = get_random_from_range(FOLLOWER_TIMER_RANGE[0],FOLLOWER_TIMER_RANGE[1])
        self.follower_timer = threading.Timer(follower_timer_interval, self.switch_to_candidate)
        self.follower_timer.start()
        logging.info("Follower timer starts. Interval:" + str(follower_timer_interval)[0:5] + " seconds.")

    def start_election(self):
        logging.info("Start election.")
        self.cur_term += 1
        self.vote_for = self.node_id

        self.vote_count = 1
        for port in self.node_port:
            node = self.node_port.index(port)
            if node != self.node_id:
                logging.info("Calling RPC(request_vote) on node" + str(node))
                _thread.start_new_thread(self.call_request_vote_on,(port,))

    def call_request_vote_on(self, port):
        node = self.node_port.index(port)
        try:
            conn = rpyc.connect("localhost", port, config={"allow_all_attrs": True})
            term, vote_granted = conn.root.request_vote(self.cur_term, self.node_id)
        except ConnectionRefusedError as e:
            vote_granted = False
            term = -1

        if vote_granted:
            self.vote_count += 1
            logging.info("Call RPC(request_vote) on node" + str(node) + " granted.")
        elif term > self.cur_term:
            # there is a node with larger term, vote for that
            logging.info("Call RPC(request_vote) on node" + str(node) + " not granted. Reason: larger term: " +
                         str(term) + ", while local cur_tern is: " + str(self.cur_term))
            self.cur_term = term
            self.vote_for = -1
            logging.info("Set leader to node" + str(node))
        elif term == -1:
            logging.info("Call RPC(request_vote) on node" + str(node) + " not granted. Reason: connection refused.")
        else:
            # term <= self.cur_term
            logging.info("Call RPC(request_vote) on node" + str(node) + " not granted. Reason: already voted.")

        if self.vote_count >= self.no_of_nodes / 2 + 1:
            self.switch_to_leader()

    def broadcast_heartbeat_periodically(self):
        self.broadcast_heartbeat_once()

        leader_timer_interval = get_random_from_range(HEARTBEAT_TIMER_RANGE[0],HEARTBEAT_TIMER_RANGE[1])
        logging.info("Heartbeat timer interval: "+str(leader_timer_interval)[0:5]+" seconds.")
        self.leader_timer = threading.Timer(leader_timer_interval, self.broadcast_heartbeat_periodically)
        self.leader_timer.start()

    def broadcast_heartbeat_once(self):
        if self.cur_leader == self.node_id:
            logging.info("Current node is leader. Broadcast heartbeat to all nodes.")
            for port in self.node_port:
                node = self.node_port.index(port)
                if node != self.node_id:
                    _thread.start_new_thread(self.call_append_entries_on,(port,))

    def call_append_entries_on(self, port):
        node = self.node_port.index(port)
        conn = rpyc.connect("localhost", port, config={"allow_all_attrs": True})
        term, success = conn.root.append_entries(self.cur_term, self.node_id)
        logging.info("Call RPC(append_entries) on node" + str(node) + ". Return: term:" + str(term) + ", success:" + str(success))
        if term > self.cur_term:
            logging.info("When sending append_entries, it finds node"+str(node)+
                " has larger term. Add leader_step_down event to mq.")
            self.leader_step_down(self.node_port.index(port))
    '''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
    '''

    def exposed_is_leader(self):
        return self.cur_leader == self.node_id


if __name__ == '__main__':
    config = sys.argv[1]
    node_id = int(sys.argv[2])
    port_no = int(sys.argv[3])

    log_filename = "log_node" + str(node_id) + ".log"
    logging.basicConfig(filename=log_filename,level=logging.DEBUG,
         format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%H:%M:%S')

    logging.info("Run on port: "+str(port_no))

    from rpyc.utils.server import ThreadedServer
    server = ThreadedServer(RaftNode(config, node_id), port=port_no, protocol_config={'allow_public_attrs': True,})
    server.start()

