<?php

namespace MySQLReplication\BinLog;

use MySQLReplication\Config\Config;
use MySQLReplication\Gtid\GtidException;
use MySQLReplication\Repository\RepositoryInterface;
use MySQLReplication\Socket\SocketException;
use MySQLReplication\Socket\SocketInterface;

class BinlogLocalFile extends BinLogSocketConnect
{
    private $pos = 0;
    private $fp = null;
    private $max_file_len = 0;

    private const HEADER_SIZE = 19;

    /**
     * @throws BinLogException
     * @throws GtidException
     * @throws SocketException
     */
    public function __construct(
        RepositoryInterface $repository,
        SocketInterface $socket
    ) {
        $this->repository = $repository;
        $this->socket = $socket;
        $this->binLogCurrent = new BinLogCurrent();

        $this->socket->connectToStream(Config::getHost(), Config::getPort());
        BinLogServerInfo::parsePackage($this->getResponse(false), $this->repository->getVersion());
        $this->authenticate();

        $this->checkSum = $this->repository->isCheckSum();

        $this->binLogCurrent->setBinLogPosition(Config::getLocalBinLogPosition());
        $this->binLogCurrent->setBinFileName(Config::getLocalBinLogFileName());

        if (!file_exists(Config::getLocalBinLogFileName()))
            throw new BinLogException('Local File Not Found!', 1);

        if (!$this->fp = fopen(Config::getLocalBinLogFileName(), 'r'))
            throw new BinLogException('File don\'t open!', 2);

        $magick = fread($this->fp, 4);

        if (bin2hex($magick) !== "fe62696e")
            throw new BinLogException('InValid BinLog File!', 3);

        $this->max_file_len = filesize(Config::getLocalBinLogFileName()) - self::HEADER_SIZE;
        $this->pos = ftell($this->fp);

        if (Config::getLocalBinLogPosition() > 0)
        {
            $this->pos += Config::getLocalBinLogPosition();
            fseek($this->fp, $this->pos);
        }
    }

    /**
     * @throws BinLogLocalFileEndException
     */
    public function getLocalResponse(): string
    {
        if ($this->pos < $this->max_file_len)
        {
            $head = fread($this->fp, self::HEADER_SIZE);
            $head_info = unpack('Vtime/Ctype/Vserverid/Vsize/Vnext/vflags', $head);
            $event_data = fread($this->fp, $head_info['size'] - self::HEADER_SIZE);

            $this->pos += $head_info['size'];

            // There is a magic number in the front, because there will be an EOF package check at the front of the internal parsing. All of them are empty, which means that the package is not finished
            return ' '.$head.$event_data;
        }

        throw new BinLogLocalFileEndException();
    }
}