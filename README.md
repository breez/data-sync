# sdk-data-sync
A simple service to synchronize user data across different apps.
The service has two endpoints:
1. SetRecord - create/update a specific record that supports conflict detection
2. ListChanges - list changes since a specific version.

# TODOs
- [ ] gRPC stream endpoint to get realtime changes.
- [ ] Distribute sqlite files accross different folders.
- [ ] DDoS protection.
