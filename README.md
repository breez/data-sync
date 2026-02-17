# sdk-data-sync
A simple service to synchronize user data across different apps.
The service has the following endpoints:
1. SetRecord - create/update a specific record that supports conflict detection
2. ListChanges - list changes since a specific revision
3. TrackChanges - listen in real-time for new changes
4. SetLock - acquire or release a named distributed lock (with TTL-based expiration)
5. GetLock - check if a named lock is currently held by any instance

# TODOs
- [ ] Distribute sqlite files accross different folders.
- [ ] DDoS protection.
