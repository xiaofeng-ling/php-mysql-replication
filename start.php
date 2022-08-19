<?php

use AlibabaCloud\Client\Signature\ShaHmac1Signature;
use MySQLReplication\BinaryDataReader\BinaryDataReader;
use MySQLReplication\BinLog\BinLogLocalFileEndException;
use MySQLReplication\Config\ConfigBuilder;
use MySQLReplication\Event\DTO\EventDTO;
use MySQLReplication\Event\EventSubscribers;
use MySQLReplication\MySQLReplicationFactory;

require_once "vendor/autoload.php";

class EventHandler extends EventSubscribers
{
    private $start_timestamp = 0;
    public function __construct($start_timestamp = 0)
    {
        $this->start_timestamp = $start_timestamp;
    }

    public function allEvents(EventDTO $event): void
    {
        $event_time_stamp = $event->getEventInfo()->getTimestamp();

//        echo $event->getType(), PHP_EOL;
        echo $event->getEventInfo()->getBinLogCurrent()->getBinFileName(),PHP_EOL;

//        if ($event_time_stamp < $this->start_timestamp)
//            return ;

        if ($event->getType() == 'update' || $event->getType() == 'insert')
        {
            echo json_encode($event, JSON_PRETTY_PRINT);
        }
        // all events got __toString() implementation
//            echo $event;

        // all events got JsonSerializable implementation
//            echo json_encode($event, JSON_PRETTY_PRINT);

//            echo 'Memory usage ' . round(memory_get_usage() / 1048576, 2) . ' MB' . PHP_EOL;
    }
}

$id = 'LTAI5t6nT4SfPAtweim97Rcr';
$key = '5fLtNXZ5BL90zGKovlRNmIECBIJIAd';
$db = 'rdsz2yzmabqiqnm';

// 签名计算
$params = [
    'Action' => 'DescribeBinlogFiles',
    'DBInstanceId' => $db,
    'EndTime' => '2022-08-19T00:00:00Z',
    'StartTime' => '2022-08-18T00:00:00Z',
    'PageSize' => 30,
    'PageNumber' => 1,
    'Format' => 'json',
    'Version' => '2014-08-15',
    'AccessKeyId' => $id,
    'SignatureMethod' => 'HMAC-SHA1',
    'Timestamp' => date("Y-m-d\\TH:i:s\\Z", time()-8*3600),
    'SignatureVersion' => '1.0',
    'SignatureNonce' => rand(0, 9999),
];

ksort($params);

$str = 'GET&%2F&' . urlencode(http_build_query($params));
$sign_old = base64_encode(hash_hmac('sha1', $str, $key, true));
$sign = (new ShaHmac1Signature())->rpc($key, 'GET', $params);

$request_query = http_build_query($params).'&Signature='.$sign;

var_dump("http://rds.aliyuncs.com?".$request_query);
die;
/** =================== 签名计算 END ======================= **/

//$fp = fopen('/mnt/hgfs/c/Users/xiaofeng/Downloads/mysql-bin.000039', 'r');
//$buffer = '';
//$buffer_pos = 0;
//$max_read_len = 32*1024*1024;       // 一次读取32M数据
//$file_size = filesize('/mnt/hgfs/c/Users/xiaofeng/Downloads/mysql-bin.000039')-19;
//$pos = 4;
//
//fseek($fp, 4);
//$timestamp = strtotime('2022-08-10');
//$buffer = fread($fp, $max_read_len);
//
//while ($pos < $file_size)
//{
//    if ($buffer_pos > strlen($buffer)-19)
//    {
//        fseek($fp, $pos);
//        $buffer = fread($fp, $max_read_len);
//        $buffer_pos = 0;
//    }
//
//    $head = substr($buffer, $buffer_pos, 19);
//    $head_info = unpack('Vtime/Ctype/Vserverid/Vsize/Vnext/vflags', $head);
//
//    $pos = $head_info['next'];
//    $buffer_pos += $head_info['size'];
//
//    if ($head_info['time'] >= $timestamp)
//    {
//        echo "OK\n";
//        die(var_dump($head_info));
//    }
//}


$binLogStream = new MySQLReplicationFactory(
    (new ConfigBuilder())
        ->withUser('root')
        ->withHost('192.168.0.251')
        ->withPassword('meimei')
        ->withPort(3306)
        ->withSlaveId(100)
        ->withBinLogFileName('mysql-bin.000038')
//        ->withBinLogPosition(329)
        ->withBinLogPosition(4)
        ->withHeartbeatPeriod(2)
        ->build()
);

//$conn = $binLogStream->getDbConnection();
//
//$rs = $conn->executeQuery('show binary logs;');
//die(var_dump($rs->fetchAllAssociative()));


//$binLogStream = new \MySQLReplication\MySQLReplicationLocalFactory(
//    (new ConfigBuilder())
//        ->withUser('root')
//        ->withHost('192.168.0.251')
//        ->withPassword('meimei')
//        ->withPort(3306)
//        ->withSlaveId(100)
//        ->withLocalBinLogFileName('/mnt/hgfs/c/Users/xiaofeng/Downloads/mysql-bin.000039')
////        ->withLocalBinLogPosition(5)
//        ->withHeartbeatPeriod(2)
//        ->build()
//);

/**
 * Register your events handler
 * @see EventSubscribers
 */
$binLogStream->registerSubscriber(new EventHandler(strtotime("2022-08-17")));

// start consuming events
$binLogStream->run();