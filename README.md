# [COMP90015](https://handbook.unimelb.edu.au/subjects/comp90015/) Distributed Systems

## Group Project (1 & 2)

### Semester 1, 2017 - University of Melbourne

---

#### Main classes

```EZShare.Server``` and ```EZShare.Client```

#### Specifications

- [Project 1](specs/proj1.pdf)
- [Project 2](specs/proj2.pdf)

#### Team: AALT | Members

- [AB](https://github.com/abhineet-gupta)
- [Alex](https://github.com/alexZhangKai)
- [Lisha](https://github.com/Lisharabbit)
- [Titus](https://github.com/titusching)
---

#### Sample Runs

- **Start the server**, in debug mode with its own IP address provided

    ````java -cp ezshare.jar EZShare/Server -debug -advertisedhostname 100.98.55.22````

- **Publish** a resource to server via client, using secure channel

    ```java -cp ezshare.jar EZShare/Client -debug -publish -uri http://remote-pub-res1.com -host 100.98.55.22 -secure```

- **Share a file** on the server, using unsecure channel

    ```java -cp ezshare.jar EZShare/Client -debug -share -host localhost 100.98.55.22 -uri file:///D:/GDrive/Docs/2017/Code/ds-ezshare/serverFile/testFile.png -secret 279002d56d0644ec94e1c1c1ed56738a```

- **Exchange** list of other servers with a given server, securely

    ```java -cp ezshare.jar EZShare/Client -debug -secure -host 100.98.55.22 -exchange -servers 100.98.58.247:3781```

---

### Potential Improvements

- Socket timeout counter - remove server from list after N timeouts, not straightaway
- Multithreaded propagateQuery method
- Update Client IP list method - remove entries when they expire
- Resource class to a SynchronisedList that stores JSON objects.
- Automate testing (via JUnit?)
- Add documentation (via JavaDocs?)

---

### Spec Clarifications

- The client only sends a single subscription request. This is just to make your client easier to build. If you wanted you could build the client to enable it to send multiple subscription requests, so long as it works exactly as intended in the project specification for testing purposes.

- The id field of the subscription request is not given by the user, it is just something that the client can make up. It could just as well be "X". The field is more useful when one server subscribes to another server, since it might bundle a bunch of subscription requests on the one connection, depending on your implementation.

- The id field does not have to be unique over the clients since each client's connection itself is used to discriminate between ids.

- Whenever a resource is updated or a new one is created then the subscription should hit it and it should be sent back to the client (even if the update did not change anything, unless an error occurred and the update did not go through). This counts as part of the result size. However removing a resource should not lead to anything being sent to the client.

- Some error conditions were not explicitly stated. Generally, the project is following an RR protocol, where every request has a response. So each subscription request should have a response. As well there can be error responses. Follow the pattern we've been following so far in the case where it is not explicitly stated what to do.
