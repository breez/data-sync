# sdk-data-sync
A simple service to synchronize user data across different apps.
The service has two endpoints:
1. SetRecord - create/update a specific record that supports conflict detection
2. ListChanges - list changes since a specific revision
3. TrackChanges - listen in real-time for new changes

# TODOs
- [ ] Distribute sqlite files accross different folders.
- [ ] DDoS protection.
