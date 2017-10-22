DJJob
=====

DJJob allows java applications to process long-running tasks asynchronously. It is a java port of [delayed_job](http://github.com/tobi/delayed_job) (developed at Shopify), which has been used in production at SeatGeek since April 2010.

Like delayed_job, DJJob uses a `jobs` table for persisting and tracking pending, in-progress, and failed jobs.

Requirements
------------

- java 1.6+
- MySqlJDBC
- commons-dbutils

Setup
-----

Import the sql database table.

```
mysql db < jobs.sql
```

The `jobs` table structure looks like:

```sql
CREATE TABLE `jobs` (
`id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
`handler` TEXT NOT NULL,
`queue` VARCHAR(255) NOT NULL DEFAULT 'default',
`attempts` INT UNSIGNED NOT NULL DEFAULT 0,
`run_at` DATETIME NULL,
`locked_at` DATETIME NULL,
`locked_by` VARCHAR(255) NULL,
`failed_at` DATETIME NULL,
`error` TEXT NULL,
`created_at` DATETIME NOT NULL
) ENGINE = INNODB;
```

> You may need to use BLOB as the column type for `handler` if you are passing in serialized blobs of data instead of record ids. For more information, see [this link](https://php.net/manual/en/function.serialize.php#refsect1-function.serialize-returnvalues) This may be the case for errors such as the following: `unserialize(): Error at offset 2010 of 2425 bytes`

Tell DJJob how to connect to your database:

```java
Map<String,String> configure = new HashMap<String,String>();
configure.put("driver", "mysql");
configure.put("host", "127.0.0.1");
configure.put("dbname", "djjob");
configure.put("user", "root");
configure.put("password", "topsecret");
DJJob.configure(configure);
```


Usage
-----

Jobs are PHP objects that respond to a method `perform`. Jobs are serialized and stored in the database.

```java
// Job class
class HelloWorldJob {
    private String name;
    
    public HelloWorldJob(String name) {
        this.name = name;
    }
    public void perform() {
        System.out.println(String.formate("Hello %s!", this.name));
    }
}

// enqueue a new job
DJJob.enqueue(new HelloWorldJob("delayed_job"));
```

Unlike delayed_job, DJJob does not have the concept of task priority (not yet at least). Instead, it supports multiple queues. By default, jobs are placed on the "default" queue. You can specifiy an alternative queue like:

```java
DJJob.enqueue(new SignupEmailJob("dev@seatgeek.com"), "email");
```

At SeatGeek, we run an email-specific queue. Emails have a `sendLater` method which places a job on the `email` queue. Here's a simplified version of our base `Email` class:

```java
class Email {
    private String recipient;
    
    public Email(String recipient) {
        this.recipient = recipient;
    }
    public void send() {
        // do some expensive work to build the email: geolocation, etc..
        // use mail api to send this email
    }
    public void perform() {
        this.send();
    }
    public function sendLater() {
        DJJob.enqueue(this, "email");
    }
}
```

Because `Email` has a `perform` method, all instances of the email class are also jobs.

Running the jobs
----------------

Running a worker is as simple as:

```java
DJWorker worker = new DJWorker(options);
worker->start();
```

Changes
-------

- Use the prefixName to distinguish workers in the same machine.
