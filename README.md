# Kagecal 

**Team**: Sam Neisewander, Leonardo Molina  
**Course**: CSE 40771 - Distributed Systems, Spring 2026

# Repisotry Structure 
```bash 
DistributedCalendar/
├── DistributedCalendar
│   ├── Calendar.py             # Core calendar logic
│   ├── Client.py               # Client-side RPC logic
│   ├── Peer.py                 # Peer-to-peer coordination 
│   ├── PersistantCalendar.py   # Txn + ckpt layer 
│   ├── Server.py               # Server-side RPC, leader, follower logic 
│   └── __init__.py             # Packages module 
├── docs                        # Documentation 
├── kagecal.py                  # Entry point/ CLI 
├── requirements.txt            # Dependencies 
└── tests                       # Unit and integration tests
```

## License

Academic project for CSE 40771. Do not distribute or copy without permission.

---

_This project follows the structure and guidelines provided by the CSE 40771 course materials._