<?php declare(strict_types=1);
class RWPDO extends PDO {
  public function __construct($host = 'localhost', $db = 'dev', $port = 4566, $user = 'root', $password = '') {
    $dsn = "pgsql:host=$host;dbname=$db;port=$port";
    parent::__construct($dsn, $user, $password);
    print("connect risingwave!\n");
  }
}
?>
