# Calendar 
1. Tested using pytest, everything seems to be working 
2. adding get_event, list_events, search_events, don't know which ones ill need but well see 

# PersistentCalendar.py 
1. Adding arguments to be passed into the constructor, txn_log_path, and ckpt_path, this was for peers to have different txn and ckpt files. Also thought you could do different directories for this but I didnt wnat a directory + two files per peer 
2. txn_log dumps them without '\n' char, it does this by placing a header then reads the txn and yields them.
3. started using unpacking of dictionaries to pass in events, since evnets is dataclass it natrually inherits `__dict__` operation which was used. 

# Client, Server, Peer 
1. My chopped code lives here, will change in the next few iterations 