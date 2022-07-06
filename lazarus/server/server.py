import zmq
import shelve
from lazarus.common.protocol import ServerMsg, ClientMsg, MessageType, NO_SESSION


#TODO: Cambiar prints por logger

#TODO: This is only for testing purposes
def explode():
    exit()


class Server:
    # address -> localhost:8000
    def __init__(
        self, 
        port: str, 
        is_leader: bool
    ):
        self.context = zmq.Context.instance()  # type: ignore
        self.context.setsockopt(zmq.LINGER, 0)
        self.rep = self.context.socket(zmq.REP)
        self.rep.bind(f"tcp://*:{port}")
        self.current_session = NO_SESSION
        self.completed_sessions = 0 
        self.on_creation_session = 0
        self.work_done_count = 0
        self.is_leader = is_leader
        if is_leader:
            self.db = shelve.open('/app/db-data/server-db', writeback=True)
            self.load_state()

        print(f"Server started on {port}")

    # This is only for testing
    def write_state(self):
        state = {
            'completed_sessions': self.completed_sessions,
            'current_session': self.current_session,
            'work_done_count': self.work_done_count
        }

        self.db['state'] = state
        self.db.sync()

        print("State Written")

    def load_state(self):
        if 'state' not in self.db:
            return
        state = self.db['state']

        self.completed_sessions = state.get('completed_sessions', 0)
        self.current_session = state.get('current_session', 0)
        self.work_done_count = state.get('work_done_count', 0)

        print("Loading state from DB:")
        print(f"- Completed Sessions: {self.completed_sessions}")
        print(f"- Current Session: {self.current_session}")
        print(f"- Work Done Count: {self.work_done_count}")


    def run(self):
        while True:
            try:
                m = self.rep.recv_string()
                self.__handle_new_message(ClientMsg.decode(m))
            except Exception as e:
                print(f"Exception on server {e}")


    def __handle_new_message(self, msg: ClientMsg):

        if not self.is_leader:
            leader = {
                'host': 'server1'
            }
            self.rep.send_string(ServerMsg(MessageType.REDIRECT, payload=leader).encode())
            return


        handlers = {
            MessageType.PROBE: self.__handle_probe,
            MessageType.SYN: self.__handle_syn,
            MessageType.SYNCHECK: self.__handle_syncheck,
            MessageType.RESULT: self.__handle_result,
            MessageType.FIN: self.__handle_fin
        }

        if not msg.mtype in handlers:
            self.rep.send_string(ServerMsg(MessageType.INVALMSG).encode())
            return

        handler = handlers[msg.mtype]
        handler(msg)

    def __handle_probe(self, _msg: ClientMsg):
        print("Received PROBE")
        self.rep.send_string(ServerMsg(MessageType.PROBEACK).encode())

    def __handle_syn(self, _msg: ClientMsg):
        print("Received SYN")
        # We don't want to persist any data here, the session has not been created yet
        if self.current_session != NO_SESSION:
            self.rep.send_string(ServerMsg(MessageType.NOTAVAIL).encode())
            return

        self.on_creation_session = self.completed_sessions + 1

        session_data = {
            'session_id': self.on_creation_session,
        }

        self.rep.send_string(ServerMsg(MessageType.SYNACK, payload=session_data).encode())

    def __handle_syncheck(self, msg: ClientMsg):
        print("Received SYNCHECK")
        # Queremos que si el id del mensaje coincide con el de la sesión actual, confirmarle
        if self.current_session != msg.session_id:
            if self.current_session != NO_SESSION or msg.session_id != self.on_creation_session:
                self.rep.send_string(ServerMsg(MessageType.INVALSESSION).encode())
                return

            self.current_session = self.on_creation_session


        # TODO: This should not be hardcoded
        session_data = {
            'session_id': self.current_session,
            'data_address': 'rabbitmq',
            'posts_exchange': 'posts',
            'comments_exchange': 'comments',
            'posts_consumer_count': 3,
            'comments_consumer_count': 3,
        }

        # TODO: Persist here
        self.write_state()

        self.rep.send_string(ServerMsg(MessageType.CHECKACK, payload=session_data).encode())

    def __handle_result(self, msg: ClientMsg):
        print("Received RESULT")
        # Here we want to check if computation has finished
        if self.current_session != msg.session_id:
            self.rep.send_string(ServerMsg(MessageType.INVALSESSION).encode())
            return

        if not self.__work_done():
            print("Work not done")
            self.rep.send_string(ServerMsg(MessageType.NOTDONE).encode())
            return

        computation_result = self.__get_computation_results()

        self.rep.send_string(ServerMsg(MessageType.RESRESP, payload=computation_result).encode())



    # Fin will always response FINACK, except when it's on a leader-election
    def __handle_fin(self, msg: ClientMsg):
        print("Received FIN")
        if self.current_session != msg.session_id:
            self.rep.send_string(ServerMsg(MessageType.FINACK).encode())
            return

        # Reset current session, including computation results
        self.current_session = NO_SESSION
        self.completed_sessions += 1

        # TODO: Persist here
        self.write_state()

        self.rep.send_string(ServerMsg(MessageType.FINACK).encode()) # If this fails, the client will send another FIN


    def __work_done(self):
        self.work_done_count += 1
        return self.work_done_count % 10 == 0

    def __get_computation_results(self):
        return {
            'post_score_avg': 113.25,
            'best_meme': '-as9idj2jpoisadjc81jlkfsa',
            'education_memes': [
                'http://veryfunny.com',
                'http://veryfunny/school.com',
                'http://veryfunny/teaching.com',
            ]
        }
