<?php

namespace MySQLReplication;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception;
use MySQLReplication\BinLog\BinLogException;
use MySQLReplication\BinLog\BinlogLocalFile;
use MySQLReplication\BinLog\BinLogSocketConnect;
use MySQLReplication\Cache\ArrayCache;
use MySQLReplication\Config\Config;
use MySQLReplication\Config\ConfigException;
use MySQLReplication\Event\Event;
use MySQLReplication\Event\EventLocal;
use MySQLReplication\Event\RowEvent\RowEventFactory;
use MySQLReplication\Gtid\GtidException;
use MySQLReplication\Repository\MySQLRepository;
use MySQLReplication\Repository\RepositoryInterface;
use MySQLReplication\Socket\Socket;
use MySQLReplication\Socket\SocketException;
use MySQLReplication\Socket\SocketInterface;
use Psr\SimpleCache\CacheInterface;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

class MySQLReplicationLocalFactory extends MySQLReplicationFactory
{
    /**
     * @throws BinLogException
     * @throws ConfigException
     * @throws Exception
     * @throws SocketException
     * @throws GtidException
     */
    public function __construct(
        Config $config,
        RepositoryInterface $repository = null,
        CacheInterface $cache = null,
        EventDispatcherInterface $eventDispatcher = null,
        SocketInterface $socket = null
    ) {
        $config::validate();

        if (null === $repository) {
            $this->connection = DriverManager::getConnection(
                [
                    'user' => Config::getUser(),
                    'password' => Config::getPassword(),
                    'host' => Config::getHost(),
                    'port' => Config::getPort(),
                    'driver' => 'pdo_mysql',
                    'charset' => Config::getCharset()
                ]
            );
            $repository = new MySQLRepository($this->connection);
        }
        if (null === $cache) {
            $cache = new ArrayCache();
        }

        $this->eventDispatcher = $eventDispatcher ?: new EventDispatcher();

        if (null === $socket) {
            $socket = new Socket();
        }

        $this->event = new EventLocal(
            new BinlogLocalFile(
                $repository,
                $socket
            ),
            new RowEventFactory(
                $repository,
                $cache
            ),
            $this->eventDispatcher,
            $cache
        );
    }
}