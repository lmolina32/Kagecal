# bully algorithm 
- pid don't change for lifetime of peer 
- maintain a logic clock, each peer has a clock 
- leader maintains the highest clock, ckpoint and transaction 


- writes are blocked during election, reads should be fine 
- if leader dies 
    - start an election, new leader is elected, send requests to each peer 
    - if peer has higher logic clock than leader, send requests with logic clock and state of calendar 

- if leader is still alive 
    - elect new leader, new leader sends addresses out, leader should reply with logic clock and state of calendar if the peer that was elected didn't have the most up to date clock 


NOTE: if leader dies, comes back to life, and has a higher logic clock than the new leader, we assume that everyone agreed that the new leader is the most up to date calendar. Thus, the leader syncs with the new leader and updates its calendar even if it is from a previous state. This is so solve the issue where the old leader has a higher logical clock, and the new leader has a lower logical clock. Then, if the new leader writes new events and the old leader comes back to life,
it is missing events from the calendar. 
