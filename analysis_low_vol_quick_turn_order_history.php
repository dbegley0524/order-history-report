<?php
@set_time_limit(300);
@ini_set('max_execution_time', '300');
@ini_set('memory_limit', '256M');

if (session_id() === '') { session_start(); }
require('../connect-db.php');

/* ---------------- HARD CUTOFF ---------------- */
$HARD_START_DATE = '2020-01-01';

/* ---------------- HELPERS ---------------- */
function h($v) { return htmlspecialchars((string)$v, ENT_QUOTES); }

function monthAbbr($m) {
    static $n = array('', 'Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec');
    $m = (int)$m;
    return isset($n[$m]) ? $n[$m] : '';
}

function month_sorter($a, $b) {
    $ay = isset($a['yr']) ? (int)$a['yr'] : 0;
    $by = isset($b['yr']) ? (int)$b['yr'] : 0;
    $am = isset($a['mo']) ? (int)$a['mo'] : 0;
    $bm = isset($b['mo']) ? (int)$b['mo'] : 0;

    if ($ay != $by) {
        return ($ay > $by) ? -1 : 1;
    }
    return ($am > $bm) ? -1 : ($am < $bm ? 1 : 0);
}

function trend_arrow($curr, $prev) {
    if ($prev === null) return '';
    $c = (float)$curr;
    $p = (float)$prev;
    if ($c > $p) return '🠉';
    if ($c < $p) return '🠋';
    return '🠊';
}

function oh_compute_repeat_meta(&$row) {
    if (!isset($row['months']) || !is_array($row['months'])) $row['months'] = array();

    $seen = array();
    for ($j=0; $j<count($row['months']); $j++) {
        $m = $row['months'][$j];
        $yr = isset($m['yr']) ? (int)$m['yr'] : 0;
        $mo = isset($m['mo']) ? (int)$m['mo'] : 0;
        if ($yr > 0 && $mo > 0) $seen[$yr.'-'.$mo] = 1;
    }

    $row['year_repeat_months'] = count($seen);
    $row['is_repeat'] = ($row['year_repeat_months'] >= 2) ? 1 : 0;
}

function oh_num_or_null($v) {
    $v = trim((string)$v);
    if ($v === '') return null;
    if (!is_numeric($v)) return null;
    return (float)$v;
}

function oh_piece_hours($year_hrs, $year_qty) {
    $q = (float)$year_qty;
    if ($q <= 0) return 0.0;
    return (float)$year_hrs / $q;
}

function oh_part_filter_sql($connection, $rawPart) {
    $rawPart = trim((string)$rawPart);
    if ($rawPart === '') return '';

    $tmp = str_replace(array("\r","\n","\t"), ' ', $rawPart);
    $tmp = preg_replace('/\s+/', ' ', $tmp);
    $tmp = str_replace(';', ',', $tmp);

    if (strpos($tmp, '*') !== false || strpos($tmp, '%') !== false) {
        $pat = str_replace('*', '%', $tmp);
        $pat = mysqli_real_escape_string($connection, $pat);
        return " AND wd.assemblyNumber LIKE '".$pat."' ";
    }

    $tokens = preg_split('/[,\s]+/', $tmp);
    $parts = array();
    for ($i=0; $i<count($tokens); $i++) {
        $t = trim($tokens[$i]);
        if ($t === '') continue;
        $parts[] = $t;
    }
    if (count($parts) === 0) return '';

    if (count($parts) === 1) {
        $one = mysqli_real_escape_string($connection, $parts[0]);
        return " AND wd.assemblyNumber = '".$one."' ";
    }

    $safe = array();
    for ($i=0; $i<count($parts); $i++) {
        $safe[] = "'".mysqli_real_escape_string($connection, $parts[$i])."'";
    }
    return " AND wd.assemblyNumber IN (".implode(',', $safe).") ";
}

/* ---------------- AUTH ---------------- */
$auth_ok  = (isset($_SESSION['oh_auth']) && $_SESSION['oh_auth'] == 1);
$auth_msg = '';

$users = array();
$uRes = mysqli_query($connection, "
    SELECT employeeNumber, firstName, lastName
    FROM labortracks.users
    WHERE order_history = 1 AND active = 1
    ORDER BY firstName, lastName
");
if ($uRes) {
    while ($r = mysqli_fetch_assoc($uRes)) { $users[] = $r; }
}

/* ---------------- DEFAULTS ---------------- */
if ($auth_ok && $_SERVER['REQUEST_METHOD'] !== 'POST') {
    $_POST['years_back'] = 2;
    $_POST['only_repeats'] = '1';
    $_POST['split_by_customer'] = '1';
    $_POST['customer_filter'] = array('3000025','3000026','300062','m1000');
    $_POST['sort_by'] = 'qty_desc';
    $_POST['sort_year'] = (int)date('Y');
    $_POST['part_filter'] = '';
}

/* ---------------- LEAN AJAX ENDPOINT ---------------- */
if ($auth_ok && $_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['action']) && $_POST['action'] === 'lean_add') {
    header('Content-Type: application/json; charset=utf-8');

    $raw = isset($_POST['payload']) ? (string)$_POST['payload'] : '';
    $data = json_decode($raw, true);

    if (!is_array($data) || !isset($data['parts']) || !is_array($data['parts'])) {
        echo json_encode(array('ok'=>0,'msg'=>'Invalid payload.'));
        exit;
    }

    $cols = array();
    $cRes = mysqli_query($connection, "
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'analysis_lean_cell_part_numbers'
    ");
    if ($cRes) {
        while ($cr = mysqli_fetch_row($cRes)) { $cols[(string)$cr[0]] = 1; }
        mysqli_free_result($cRes);
    }

    if (!isset($cols['part_number'])) {
        echo json_encode(array('ok'=>0,'msg'=>"analysis_lean_cell_part_numbers.part_number not found."));
        exit;
    }

    $notes = isset($data['notes']) ? trim((string)$data['notes']) : '';
    $added = 0; $updated = 0; $skipped = 0;

    $use_customer_numbers = isset($cols['customer_numbers']);
    $use_year1_yyyy = isset($cols['year1_yyyy']);
    $use_year1_qty  = isset($cols['year1_qty']);
    $use_year2_yyyy = isset($cols['year2_yyyy']);
    $use_year2_qty  = isset($cols['year2_qty']);
    $use_year3_yyyy = isset($cols['year3_yyyy']);
    $use_year3_qty  = isset($cols['year3_qty']);
    $use_notes      = isset($cols['notes']);

    $use_user_name     = isset($cols['user_name']);
    $use_user_password = isset($cols['user_password']);
    $use_date_added    = isset($cols['date_added']);
    $use_user_ip       = isset($cols['user_ip']);

    $session_emp = isset($_SESSION['oh_emp']) ? (string)$_SESSION['oh_emp'] : '';
    $user_ip = isset($_SERVER['REMOTE_ADDR']) ? (string)$_SERVER['REMOTE_ADDR'] : '';

    foreach ($data['parts'] as $pRow) {
        if (!is_array($pRow)) { $skipped++; continue; }
        $part = isset($pRow['part']) ? trim((string)$pRow['part']) : '';
        if ($part === '') { $skipped++; continue; }

        $customer_numbers = isset($pRow['customers']) ? trim((string)$pRow['customers']) : '';

        $y1 = isset($pRow['y1']) ? (int)$pRow['y1'] : 0;
        $q1 = isset($pRow['q1']) ? (int)$pRow['q1'] : 0;
        $y2 = isset($pRow['y2']) ? (int)$pRow['y2'] : 0;
        $q2 = isset($pRow['q2']) ? (int)$pRow['q2'] : 0;
        $y3 = isset($pRow['y3']) ? (int)$pRow['y3'] : 0;
        $q3 = isset($pRow['q3']) ? (int)$pRow['q3'] : 0;

        $exists = 0;
        $st = mysqli_prepare($connection, "SELECT 1 FROM analysis_lean_cell_part_numbers WHERE part_number = ? LIMIT 1");
        if ($st) {
            mysqli_stmt_bind_param($st, 's', $part);
            mysqli_stmt_execute($st);
            mysqli_stmt_store_result($st);
            $exists = (mysqli_stmt_num_rows($st) > 0) ? 1 : 0;
            mysqli_stmt_close($st);
        }

        if ($exists) {
            $sets = array();
            $types = '';
            $vals = array();

            if ($use_customer_numbers) { $sets[] = "customer_numbers=?"; $types .= 's'; $vals[] = $customer_numbers; }

            if ($use_year1_yyyy) { $sets[] = "year1_yyyy=?"; $types .= 'i'; $vals[] = $y1; }
            if ($use_year1_qty)  { $sets[] = "year1_qty=?";  $types .= 'i'; $vals[] = $q1; }
            if ($use_year2_yyyy) { $sets[] = "year2_yyyy=?"; $types .= 'i'; $vals[] = $y2; }
            if ($use_year2_qty)  { $sets[] = "year2_qty=?";  $types .= 'i'; $vals[] = $q2; }
            if ($use_year3_yyyy) { $sets[] = "year3_yyyy=?"; $types .= 'i'; $vals[] = $y3; }
            if ($use_year3_qty)  { $sets[] = "year3_qty=?";  $types .= 'i'; $vals[] = $q3; }

            if ($use_notes) { $sets[] = "notes=?"; $types .= 's'; $vals[] = $notes; }

            if ($use_user_name) { $sets[] = "user_name=?"; $types .= 's'; $vals[] = $session_emp; }

            if ($use_user_ip) { $sets[] = "user_ip=?"; $types .= 's'; $vals[] = $user_ip; }

            if (count($sets) === 0) { $skipped++; continue; }

            $sqlU = "UPDATE analysis_lean_cell_part_numbers SET ".implode(', ', $sets)." WHERE part_number=?";
            $types .= 's';
            $vals[] = $part;

            $stU = mysqli_prepare($connection, $sqlU);
            if (!$stU) { $skipped++; continue; }

            $bind = array($stU, $types);
            for ($i=0; $i<count($vals); $i++) { $bind[] = &$vals[$i]; }
            call_user_func_array('mysqli_stmt_bind_param', $bind);

            mysqli_stmt_execute($stU);
            mysqli_stmt_close($stU);

            $updated++;
        } else {
            $fields = array('part_number');
            $place  = array('?');
            $types  = 's';
            $vals   = array($part);

            if ($use_customer_numbers) { $fields[]='customer_numbers'; $place[]='?'; $types.='s'; $vals[]=$customer_numbers; }

            if ($use_year1_yyyy) { $fields[]='year1_yyyy'; $place[]='?'; $types.='i'; $vals[]=$y1; }
            if ($use_year1_qty)  { $fields[]='year1_qty';  $place[]='?'; $types.='i'; $vals[]=$q1; }
            if ($use_year2_yyyy) { $fields[]='year2_yyyy'; $place[]='?'; $types.='i'; $vals[]=$y2; }
            if ($use_year2_qty)  { $fields[]='year2_qty';  $place[]='?'; $types.='i'; $vals[]=$q2; }
            if ($use_year3_yyyy) { $fields[]='year3_yyyy'; $place[]='?'; $types.='i'; $vals[]=$y3; }
            if ($use_year3_qty)  { $fields[]='year3_qty';  $place[]='?'; $types.='i'; $vals[]=$q3; }

            if ($use_user_name) { $fields[]='user_name'; $place[]='?'; $types.='s'; $vals[]=$session_emp; }

            if ($use_date_added) { $fields[]='date_added'; $place[]='NOW()'; }
            if ($use_user_ip)    { $fields[]='user_ip';    $place[]='?'; $types.='s'; $vals[]=$user_ip; }

            if ($use_notes) { $fields[]='notes'; $place[]='?'; $types.='s'; $vals[]=$notes; }

            $sqlI = "INSERT INTO analysis_lean_cell_part_numbers (".implode(',', $fields).") VALUES (".implode(',', $place).")";
            $stI = mysqli_prepare($connection, $sqlI);
            if (!$stI) { $skipped++; continue; }

            $bind = array($stI, $types);
            for ($i=0; $i<count($vals); $i++) { $bind[] = &$vals[$i]; }
            call_user_func_array('mysqli_stmt_bind_param', $bind);

            mysqli_stmt_execute($stI);
            mysqli_stmt_close($stI);

            $added++;
        }
    }

    echo json_encode(array(
        'ok' => 1,
        'msg' => "Lean update complete.",
        'added' => $added,
        'updated' => $updated,
        'skipped' => $skipped
    ));
    exit;
}

/* ---------------- LOGIN / LOGOUT ---------------- */
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['action']) && $_POST['action'] === 'login') {
    $emp = isset($_POST['user_emp']) ? trim($_POST['user_emp']) : '';
    $pw  = isset($_POST['user_pass']) ? trim($_POST['user_pass']) : '';

    if ($emp === '' || $pw === '') {
        if (!$auth_ok) {
            $auth_ok = false;
            $auth_msg = 'You are not authorized to run this report.';
        }
    } else {
        $stmt = mysqli_prepare($connection, "
            SELECT employeeNumber
            FROM labortracks.users
            WHERE employeeNumber = ?
              AND password = ?
              AND order_history = 1
              AND active = 1
            LIMIT 1
        ");
        if ($stmt) {
            mysqli_stmt_bind_param($stmt, 'is', $emp, $pw);
            mysqli_stmt_execute($stmt);
            mysqli_stmt_store_result($stmt);

            if (mysqli_stmt_num_rows($stmt) == 1) {
                $_SESSION['oh_auth'] = 1;
                $_SESSION['oh_emp']  = $emp;
                $auth_ok = true;
                $auth_msg = '';
            } else {
                $auth_ok = false;
                $auth_msg = 'You are not authorized to run this report.';
            }
            mysqli_stmt_close($stmt);
        } else {
            $auth_ok = false;
            $auth_msg = 'You are not authorized to run this report.';
        }
    }
}

if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['action']) && $_POST['action'] === 'logout') {
    unset($_SESSION['oh_auth']);
    unset($_SESSION['oh_emp']);
    $auth_ok = false;
    $auth_msg = '';
}

/* ---------------- PRESETS ---------------- */
if (!isset($_SESSION['oh_presets'])) $_SESSION['oh_presets'] = array();

if ($auth_ok && $_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['preset_action'])) {

    if (isset($_POST['preset_name_new']) && trim($_POST['preset_name_new']) !== '') {
        $_POST['preset_name'] = trim($_POST['preset_name_new']);
    }

    if (isset($_POST['customer_filter']) && is_array($_POST['customer_filter'])) {
        $clean = array();
        for ($i=0; $i<count($_POST['customer_filter']); $i++) {
            $v = (string)$_POST['customer_filter'][$i];
            if ($v === '__ALL__' || $v === '__NONE__') continue;
            $clean[] = $v;
        }
        $_POST['customer_filter'] = $clean;
    }

    $min_rep_save = 0;
    if (isset($_POST['min_repeats'])) {
        $t = trim((string)$_POST['min_repeats']);
        $t = preg_replace('/[^0-9]/', '', $t);
        if ($t !== '') $min_rep_save = (int)$t;
    }

    $max_rep_save = 0;
    if (isset($_POST['max_repeats'])) {
        $t2 = trim((string)$_POST['max_repeats']);
        $t2 = preg_replace('/[^0-9]/', '', $t2);
        if ($t2 !== '') $max_rep_save = (int)$t2;
    }

    $min_ph_save = isset($_POST['min_piece_hours']) ? $_POST['min_piece_hours'] : '';
    $max_ph_save = isset($_POST['max_piece_hours']) ? $_POST['max_piece_hours'] : '';

    $pa = $_POST['preset_action'];
    $pname = isset($_POST['preset_name']) ? trim($_POST['preset_name']) : '';

    if ($pa === 'save' && $pname !== '') {
        $_SESSION['oh_presets'][$pname] = array(
            'years_back'        => isset($_POST['years_back']) ? (int)$_POST['years_back'] : 2,
            'customer_filter'   => (isset($_POST['customer_filter']) && is_array($_POST['customer_filter'])) ? $_POST['customer_filter'] : array(),
            'min_hours'         => isset($_POST['min_hours']) ? $_POST['min_hours'] : '',
            'max_hours'         => isset($_POST['max_hours']) ? $_POST['max_hours'] : '',
            'min_piece_hours'   => $min_ph_save,
            'max_piece_hours'   => $max_ph_save,
            'min_repeats'       => $min_rep_save,
            'max_repeats'       => $max_rep_save,
            'part_filter'       => isset($_POST['part_filter']) ? trim((string)$_POST['part_filter']) : '',
            'split_by_customer' => isset($_POST['split_by_customer']) ? 1 : 0,
            'only_repeats'      => isset($_POST['only_repeats']) ? 1 : 0,
            'active_only'       => isset($_POST['active_only']) ? 1 : 0,
            'min_open_qty'      => isset($_POST['min_open_qty']) ? trim($_POST['min_open_qty']) : '',
            'max_open_qty'      => isset($_POST['max_open_qty']) ? trim($_POST['max_open_qty']) : '',
            'sort_by'           => isset($_POST['sort_by']) ? (string)$_POST['sort_by'] : 'qty_desc',
            'sort_year'         => isset($_POST['sort_year']) ? (int)$_POST['sort_year'] : (int)date('Y')
        );
    } elseif ($pa === 'delete' && $pname !== '' && isset($_SESSION['oh_presets'][$pname])) {
        unset($_SESSION['oh_presets'][$pname]);
    } elseif ($pa === 'load' && $pname !== '' && isset($_SESSION['oh_presets'][$pname])) {
        $loaded = $_SESSION['oh_presets'][$pname];

        $_POST['years_back'] = $loaded['years_back'];
        $_POST['customer_filter'] = $loaded['customer_filter'];
        $_POST['min_hours'] = $loaded['min_hours'];
        $_POST['max_hours'] = $loaded['max_hours'];
        $_POST['min_piece_hours'] = isset($loaded['min_piece_hours']) ? $loaded['min_piece_hours'] : '';
        $_POST['max_piece_hours'] = isset($loaded['max_piece_hours']) ? $loaded['max_piece_hours'] : '';
        $_POST['min_repeats'] = $loaded['min_repeats'];
        $_POST['max_repeats'] = isset($loaded['max_repeats']) ? $loaded['max_repeats'] : 0;
        $_POST['part_filter'] = isset($loaded['part_filter']) ? (string)$loaded['part_filter'] : '';
        $_POST['active_only'] = isset($loaded['active_only']) ? $loaded['active_only'] : 0;
        $_POST['min_open_qty'] = isset($loaded['min_open_qty']) ? $loaded['min_open_qty'] : '';
        $_POST['max_open_qty'] = isset($loaded['max_open_qty']) ? $loaded['max_open_qty'] : '';

        if (isset($loaded['split_by_customer']) && (int)$loaded['split_by_customer'] === 1) $_POST['split_by_customer'] = '1';
        else unset($_POST['split_by_customer']);

        if (isset($loaded['only_repeats']) && (int)$loaded['only_repeats'] === 1) $_POST['only_repeats'] = '1';
        else unset($_POST['only_repeats']);

        if (isset($loaded['sort_by'])) $_POST['sort_by'] = (string)$loaded['sort_by'];
        if (isset($loaded['sort_year'])) $_POST['sort_year'] = (int)$loaded['sort_year'];
    }
}

/* ---------------- FILTERS ---------------- */
$years_back = 2;
if (isset($_POST['years_back'])) $years_back = (int)$_POST['years_back'];
if ($years_back < 1) $years_back = 1;
if ($years_back > 12) $years_back = 12;

$split_by_customer = isset($_POST['split_by_customer']) ? 1 : 0;
$active_only = isset($_POST['active_only']) ? 1 : 0;

$min_open_qty = isset($_POST['min_open_qty']) ? trim($_POST['min_open_qty']) : '';
$max_open_qty = isset($_POST['max_open_qty']) ? trim($_POST['max_open_qty']) : '';
$minOpenQty = oh_num_or_null($min_open_qty);
$maxOpenQty = oh_num_or_null($max_open_qty);

$min_hours   = isset($_POST['min_hours']) ? trim($_POST['min_hours']) : '';
$max_hours   = isset($_POST['max_hours']) ? trim($_POST['max_hours']) : '';

$min_piece_hours = isset($_POST['min_piece_hours']) ? trim($_POST['min_piece_hours']) : '';
$max_piece_hours = isset($_POST['max_piece_hours']) ? trim($_POST['max_piece_hours']) : '';
$minPH = oh_num_or_null($min_piece_hours);
$maxPH = oh_num_or_null($max_piece_hours);

$min_repeats = 0;
if (isset($_POST['min_repeats'])) {
    $tmp = trim((string)$_POST['min_repeats']);
    $tmp = preg_replace('/[^0-9]/', '', $tmp);
    if ($tmp !== '') $min_repeats = (int)$tmp;
}
$max_repeats = 0;
if (isset($_POST['max_repeats'])) {
    $tmp2 = trim((string)$_POST['max_repeats']);
    $tmp2 = preg_replace('/[^0-9]/', '', $tmp2);
    if ($tmp2 !== '') $max_repeats = (int)$tmp2;
}

$part_filter = isset($_POST['part_filter']) ? trim((string)$_POST['part_filter']) : '';

$only_repeats = isset($_POST['only_repeats']) ? 1 : 0;

$customer_filter = array();
if (isset($_POST['customer_filter']) && is_array($_POST['customer_filter'])) {
    for ($i=0; $i<count($_POST['customer_filter']); $i++) {
        $v = (string)$_POST['customer_filter'][$i];
        if ($v === '__ALL__' || $v === '__NONE__') continue;
        $customer_filter[] = $v;
    }
}
if (count($customer_filter) === 0) {
    $customer_filter = array('3000025','3000026','300062','m1000');
}

/* ---------------- SORT CONTROLS ---------------- */
$sort_by = isset($_POST['sort_by']) ? (string)$_POST['sort_by'] : 'qty_desc';
$sort_year = isset($_POST['sort_year']) ? (int)$_POST['sort_year'] : (int)date('Y');

$allowed_sort_by = array(
    'part_asc','part_desc','cust_asc','cust_desc','qty_asc','qty_desc','hrs_asc','hrs_desc','rep_asc','rep_desc'
);
if (!in_array($sort_by, $allowed_sort_by, true)) $sort_by = 'qty_desc';
if ($sort_year <= 0) $sort_year = (int)date('Y');

/* ---------------- DATE RANGE ---------------- */
$currYear = (int)date('Y');
$startYear = $currYear - $years_back + 1;
$start_date = sprintf('%04d-01-01', $startYear);
if ($start_date < $HARD_START_DATE) $start_date = $HARD_START_DATE;

$maxDataYear = $currYear + 1;
$mxRes = mysqli_query($connection, "
    SELECT MAX(wd.poDate) AS max_date
    FROM labortracks.workorder_data wd
    WHERE wd.poDate >= '$HARD_START_DATE'
");
if ($mxRes) {
    $mxRow = mysqli_fetch_assoc($mxRes);
    mysqli_free_result($mxRes);
    if ($mxRow && isset($mxRow['max_date']) && $mxRow['max_date']) {
        $y = (int)substr((string)$mxRow['max_date'], 0, 4);
        if ($y > $maxDataYear) $maxDataYear = $y;
    }
}
$end_date = sprintf('%04d-12-31', $maxDataYear);

/* ---------------- BUILD SQL FILTERS ---------------- */
$customerSql = '';
if (count($customer_filter) > 0) {
    $safe = array();
    foreach ($customer_filter as $v) {
        $v = trim($v);
        if ($v === '') continue;
        $safe[] = "'".mysqli_real_escape_string($connection, $v)."'";
    }
    if (count($safe) > 0) $customerSql = " AND wd.customerNumber IN (".implode(',', $safe).") ";
}

$hoursSql = '';
$minH = ($min_hours !== '' && is_numeric($min_hours)) ? (float)$min_hours : null;
$maxH = ($max_hours !== '' && is_numeric($max_hours)) ? (float)$max_hours : null;

if ($minH !== null || $maxH !== null) {
    $cond = '';
    if ($minH !== null && $maxH !== null) $cond = "wd.orderHours BETWEEN ".$minH." AND ".$maxH;
    elseif ($minH !== null)               $cond = "wd.orderHours >= ".$minH;
    else                                 $cond = "wd.orderHours <= ".$maxH;

    $hoursSql = " AND (YEAR(wd.poDate) <> ".(int)$sort_year." OR (".$cond.")) ";
}

$partSql = oh_part_filter_sql($connection, $part_filter);

/* ---------------- CUSTOMER OPTIONS ---------------- */
$customerOptions = array();
$ooRes = mysqli_query($connection, "
    SELECT DISTINCT customerNumber
    FROM open_orders
    WHERE customerNumber IS NOT NULL AND customerNumber <> ''
");
if ($ooRes) {
    while ($r = mysqli_fetch_row($ooRes)) { $customerOptions[] = (string)$r[0]; }
    mysqli_free_result($ooRes);
}

$customerData = array();
if (count($customerOptions) > 0) {
    $safe = array();
    foreach ($customerOptions as $cn) {
        $safe[] = "'".mysqli_real_escape_string($connection, $cn)."'";
    }
    $cNameRes = mysqli_query($connection, "
        SELECT customer_name, id_1, id_2, id_3, id_4, id_5, id_6, id_7, id_8, id_9, id_10
        FROM customer_details
        WHERE id_1 IN (".implode(',', $safe).") OR 
              id_2 IN (".implode(',', $safe).") OR 
              id_3 IN (".implode(',', $safe).") OR 
              id_4 IN (".implode(',', $safe).") OR 
              id_5 IN (".implode(',', $safe).") OR 
              id_6 IN (".implode(',', $safe).") OR 
              id_7 IN (".implode(',', $safe).") OR 
              id_8 IN (".implode(',', $safe).") OR 
              id_9 IN (".implode(',', $safe).") OR 
              id_10 IN (".implode(',', $safe).")
    ");
    if ($cNameRes) {
        while ($cr = mysqli_fetch_assoc($cNameRes)) {
            $name = $cr['customer_name'];
            for ($i=1; $i<=10; $i++) {
                $idCol = 'id_'.$i;
                if ($cr[$idCol] && in_array($cr[$idCol], $customerOptions)) {
                    $customerData[$cr[$idCol]] = $name;
                }
            }
        }
        mysqli_free_result($cNameRes);
    }
}

/* ---------------- DATA LOAD ---------------- */
$rows = array();
$load_ms = 0;

$sql = "
SELECT
    wd.assemblyNumber AS part,
    YEAR(wd.poDate)   AS yr,
    MONTH(wd.poDate)  AS mo,
    wd.customerNumber AS cust,
    SUM(wd.quanity)   AS qty,
    SUM(wd.orderHours) AS hrs
FROM labortracks.workorder_data wd
LEFT JOIN workorder_details d
  ON wd.workorderNumber = d.workorderID
LEFT JOIN open_orders oo
  ON oo.workorderNumber = wd.workorderNumber
WHERE wd.poDate BETWEEN '$start_date' AND '$end_date'
  AND wd.poDate >= '$HARD_START_DATE'
  AND wd.orderHours > 0
  AND (
        d.currentStatus IN ('picked','complete')
        OR oo.workorderNumber IS NOT NULL
      )
  $customerSql
  $hoursSql
  $partSql
GROUP BY wd.assemblyNumber, yr, mo, wd.customerNumber
";

$start_t = microtime(true);

if ($auth_ok) {
    $res = mysqli_query($connection, $sql);
    if (!$res) { die('Query Error: '.mysqli_error($connection)); }

    $bucket = array();

    while ($r = mysqli_fetch_assoc($res)) {
        $part = (string)$r['part'];
        $yr = (int)$r['yr'];
        $mo = (int)$r['mo'];
        $cust = (string)$r['cust'];

        $qty = (float)$r['qty'];
        $hrs = (float)$r['hrs'];

        $key = $part.'|'.$yr;
        if ($split_by_customer) $key .= '|'.$cust;

        if (!isset($bucket[$key])) {
            $bucket[$key] = array(
                'part' => $part,
                'yr' => $yr,
                'cust' => ($split_by_customer ? $cust : ''),
                'custSet' => array(),
                'custList' => '',
                'months' => array(),
                'year_qty' => 0.0,
                'year_hrs' => 0.0,
                'piece_hrs' => 0.0,
                'year_repeat_months' => 0,
                'is_repeat' => 0,
                'qty_trend' => '',
                'hrs_trend' => ''
            );
        }

        $bucket[$key]['custSet'][$cust] = 1;
        $bucket[$key]['year_qty'] += $qty;
        $bucket[$key]['year_hrs'] += $hrs;

        $bucket[$key]['months'][] = array('mo' => $mo, 'yr' => $yr, 'qty' => $qty, 'hrs' => $hrs);
    }
    mysqli_free_result($res);

    foreach ($bucket as $k => $b) {
        if (!$split_by_customer) {
            $custNums = array_keys($b['custSet']);
            sort($custNums, SORT_STRING);
            $bucket[$k]['custList'] = implode(',', $custNums);
        }
        oh_compute_repeat_meta($bucket[$k]);
        $bucket[$k]['piece_hrs'] = oh_piece_hours($bucket[$k]['year_hrs'], $bucket[$k]['year_qty']);
    }

    $rows = array_values($bucket);
    unset($bucket);

    $load_ms = (int)round((microtime(true) - $start_t) * 1000.0);

    /* ----- Trend arrows ----- */
    $byKeyYear = array();
    for ($i=0; $i<count($rows); $i++) {
        $baseKey = $rows[$i]['part'];
        if ($split_by_customer) $baseKey .= '|'.$rows[$i]['cust'];
        $byKeyYear[$baseKey.'|'.$rows[$i]['yr']] = array('qty'=>$rows[$i]['year_qty'],'hrs'=>$rows[$i]['year_hrs']);
    }

    for ($i=0; $i<count($rows); $i++) {
        $baseKey = $rows[$i]['part'];
        if ($split_by_customer) $baseKey .= '|'.$rows[$i]['cust'];

        $prevYear = (int)$rows[$i]['yr'] - 1;
        $prev = null;
        if (isset($byKeyYear[$baseKey.'|'.$prevYear])) $prev = $byKeyYear[$baseKey.'|'.$prevYear];

        $rows[$i]['qty_trend'] = trend_arrow($rows[$i]['year_qty'], ($prev===null?null:$prev['qty']));
        $rows[$i]['hrs_trend'] = trend_arrow($rows[$i]['year_hrs'], ($prev===null?null:$prev['hrs']));
    }

    /* ----- Apply pattern filters ----- */
    if ($minPH !== null || $maxPH !== null) {
        $filteredPH = array();
        for ($i=0; $i<count($rows); $i++) {
            $y = (int)$rows[$i]['yr'];
            if ($y === (int)$sort_year) {
                $ph = isset($rows[$i]['piece_hrs']) ? (float)$rows[$i]['piece_hrs'] : 0.0;
                if ($minPH !== null && $ph < (float)$minPH) continue;
                if ($maxPH !== null && $ph > (float)$maxPH) continue;
            }
            $filteredPH[] = $rows[$i];
        }
        $rows = $filteredPH;
    }

    if ($min_repeats > 0 || $max_repeats > 0) {
        $filtered = array();
        for ($i=0; $i<count($rows); $i++) {
            $y = (int)$rows[$i]['yr'];
            if ($y === (int)$sort_year) {
                $rep = (int)$rows[$i]['year_repeat_months'];
                if ($min_repeats > 0 && $rep < $min_repeats) continue;
                if ($max_repeats > 0 && $rep > $max_repeats) continue;
            }
            $filtered[] = $rows[$i];
        }
        $rows = $filtered;
    }

    if ($only_repeats) {
        $filtered2 = array();
        for ($i=0; $i<count($rows); $i++) {
            $y = (int)$rows[$i]['yr'];
            if ($y === (int)$sort_year) {
                if (isset($rows[$i]['is_repeat']) && (int)$rows[$i]['is_repeat'] === 1) $filtered2[] = $rows[$i];
            } else {
                $filtered2[] = $rows[$i];
            }
        }
        $rows = $filtered2;
    }

    /* ----- Open orders for filtering active_only / min/max open qty ----- */
    $hasOpen = array();
    $ooCols = array();
    $ooCR = mysqli_query($connection, "
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'open_orders'
    ");
    if ($ooCR) {
        while ($rr = mysqli_fetch_row($ooCR)) { $ooCols[(string)$rr[0]] = 1; }
        mysqli_free_result($ooCR);
    }

    $ooPartCol = '';
    if (isset($ooCols['assemblyNumber'])) $ooPartCol = 'assemblyNumber';
    elseif (isset($ooCols['part_number'])) $ooPartCol = 'part_number';
    elseif (isset($ooCols['part'])) $ooPartCol = 'part';

    $ooCustCol = '';
    if (isset($ooCols['customerNumber'])) $ooCustCol = 'customerNumber';
    elseif (isset($ooCols['customer'])) $ooCustCol = 'customer';

    $ooQtyCol = '';
    if (isset($ooCols['quanity'])) $ooQtyCol = 'quanity';
    elseif (isset($ooCols['quantity'])) $ooQtyCol = 'quantity';

    $ooDateCol = '';
    if (isset($ooCols['poDate'])) $ooDateCol = 'poDate';
    elseif (isset($ooCols['po_date'])) $ooDateCol = 'po_date';

    if ($active_only || $minOpenQty !== null || $maxOpenQty !== null) {
        if ($ooPartCol !== '' && $ooQtyCol !== '') {
            $ooSql = "
                SELECT $ooPartCol AS part";
            if ($split_by_customer && $ooCustCol !== '') {
                $ooSql .= ", $ooCustCol AS cust";
            }
            $ooSql .= ", SUM($ooQtyCol) AS total_qty
                FROM open_orders
                WHERE $ooPartCol IS NOT NULL AND $ooPartCol <> ''
                GROUP BY $ooPartCol";
            if ($split_by_customer && $ooCustCol !== '') {
                $ooSql .= ", $ooCustCol";
            }

            $ooRes = mysqli_query($connection, $ooSql);
            if ($ooRes) {
                while ($or = mysqli_fetch_assoc($ooRes)) {
                    $p = (string)$or['part'];
                    $total = (float)$or['total_qty'];

                    $key = $split_by_customer && $ooCustCol !== '' && isset($or['cust'])
                        ? $p . '|' . (string)$or['cust']
                        : $p;

                    $hasOpen[$key] = $total;
                }
                mysqli_free_result($ooRes);
            }
        }
    }

    if ($active_only || $minOpenQty !== null || $maxOpenQty !== null) {
        $filteredActive = array();
        foreach ($rows as $row) {
            $p = $row['part'];
            $key = $split_by_customer ? $p . '|' . $row['cust'] : $p;

            if (isset($hasOpen[$key])) {
                $openTotal = (float)$hasOpen[$key];

                if (($minOpenQty !== null && $openTotal < (float)$minOpenQty) ||
                    ($maxOpenQty !== null && $openTotal > (float)$maxOpenQty)) {
                    continue;
                }

                $filteredActive[] = $row;
            } elseif (!$active_only) {
                $filteredActive[] = $row;
            }
        }
        $rows = $filteredActive;
    }

    // Collect valid parts that have the sort_year row after all filters
    $validParts = array();
    for ($i=0; $i<count($rows); $i++) {
        $p = $rows[$i]['part'];
        $y = $rows[$i]['yr'];
        if ($y === (int)$sort_year) {
            $validParts[$p] = 1;
        }
    }

    $finalRows = array();
    for ($i=0; $i<count($rows); $i++) {
        $p = $rows[$i]['part'];
        if (isset($validParts[$p])) {
            $finalRows[] = $rows[$i];
        }
    }
    $rows = $finalRows;

    /* ----- Group and sort ----- */
    $partYears = array();
    $partYearAgg = array();
    $partCustSet = array();

    for ($i=0; $i<count($rows); $i++) {
        $p = (string)$rows[$i]['part'];
        $y = (int)$rows[$i]['yr'];
        $c = $split_by_customer ? (string)$rows[$i]['cust'] : '';

        if (!isset($partYears[$p])) $partYears[$p] = array();
        if (!isset($partYears[$p][$y])) $partYears[$p][$y] = array();
        $ck = $split_by_customer ? $c : '_ALL_';
        $partYears[$p][$y][$ck] = $rows[$i];

        if (!isset($partYearAgg[$p])) $partYearAgg[$p] = array();
        if (!isset($partYearAgg[$p][$y])) {
            $partYearAgg[$p][$y] = array('qty'=>0.0,'hrs'=>0.0,'rep'=>0.0,'custMin'=>'','custMax'=>'');
        }
        $partYearAgg[$p][$y]['qty'] += (float)$rows[$i]['year_qty'];
        $partYearAgg[$p][$y]['hrs'] += (float)$rows[$i]['year_hrs'];
        if ((float)$rows[$i]['year_repeat_months'] > (float)$partYearAgg[$p][$y]['rep']) {
            $partYearAgg[$p][$y]['rep'] = (float)$rows[$i]['year_repeat_months'];
        }

        if ($split_by_customer) {
            if (!isset($partCustSet[$p])) $partCustSet[$p] = array();
            $partCustSet[$p][$c] = 1;

            if ($partYearAgg[$p][$y]['custMin'] === '' || strcmp($c, $partYearAgg[$p][$y]['custMin']) < 0) $partYearAgg[$p][$y]['custMin'] = $c;
            if ($partYearAgg[$p][$y]['custMax'] === '' || strcmp($c, $partYearAgg[$p][$y]['custMax']) > 0) $partYearAgg[$p][$y]['custMax'] = $c;
        }
    }

    $partList = array_keys($partYears);

    usort($partList, function($pa, $pb) use ($sort_by, $sort_year, $partYearAgg, $split_by_customer) {

        if ($sort_by === 'part_asc' || $sort_by === 'part_desc') {
            $cmp = strcmp((string)$pa, (string)$pb);
            return ($sort_by === 'part_desc') ? -$cmp : $cmp;
        }

        $ya = isset($partYearAgg[$pa]) && isset($partYearAgg[$pa][$sort_year]) ? $partYearAgg[$pa][$sort_year] : null;
        $yb = isset($partYearAgg[$pb]) && isset($partYearAgg[$pb][$sort_year]) ? $partYearAgg[$pb][$sort_year] : null;

        $hasA = ($ya !== null) ? 1 : 0;
        $hasB = ($yb !== null) ? 1 : 0;
        if ($hasA !== $hasB) return ($hasA < $hasB) ? 1 : -1;

        $desc = (substr($sort_by, -5) === '_desc');

        if ($sort_by === 'qty_asc' || $sort_by === 'qty_desc') {
            $na = ($ya !== null) ? (float)$ya['qty'] : 0.0;
            $nb = ($yb !== null) ? (float)$yb['qty'] : 0.0;
            if ($na != $nb) return $desc ? (($na < $nb) ? 1 : -1) : (($na < $nb) ? -1 : 1);
        } elseif ($sort_by === 'hrs_asc' || $sort_by === 'hrs_desc') {
            $na = ($ya !== null) ? (float)$ya['hrs'] : 0.0;
            $nb = ($yb !== null) ? (float)$yb['hrs'] : 0.0;
            if ($na != $nb) return $desc ? (($na < $nb) ? 1 : -1) : (($na < $nb) ? -1 : 1);
        } elseif ($sort_by === 'rep_asc' || $sort_by === 'rep_desc') {
            $na = ($ya !== null) ? (float)$ya['rep'] : 0.0;
            $nb = ($yb !== null) ? (float)$yb['rep'] : 0.0;
            if ($na != $nb) return $desc ? (($na < $nb) ? 1 : -1) : (($na < $nb) ? -1 : 1);
        } elseif ($sort_by === 'cust_asc' || $sort_by === 'cust_desc') {
            $sa = ($ya !== null) ? ($desc ? (string)$ya['custMax'] : (string)$ya['custMin']) : '';
            $sb = ($yb !== null) ? ($desc ? (string)$yb['custMax'] : (string)$yb['custMin']) : '';
            $cmp = strcmp($sa, $sb);
            if ($cmp !== 0) return $cmp;
        }

        return strcmp((string)$pa, (string)$pb);
    });

    $rows_sorted = array();

    for ($pi=0; $pi<count($partList); $pi++) {
        $p = (string)$partList[$pi];
        $years = array_keys($partYears[$p]);
        rsort($years, SORT_NUMERIC);

        for ($yi=0; $yi<count($years); $yi++) {
            $y = (int)$years[$yi];

            if ($split_by_customer) {
                $custKeys = array_keys($partYears[$p][$y]);
                sort($custKeys, SORT_STRING);
                for ($ci=0; $ci<count($custKeys); $ci++) {
                    $ck = (string)$custKeys[$ci];
                    $rows_sorted[] = $partYears[$p][$y][$ck];
                }
            } else {
                if (isset($partYears[$p][$y]['_ALL_'])) $rows_sorted[] = $partYears[$p][$y]['_ALL_'];
                else {
                    $any = array_values($partYears[$p][$y]);
                    if (count($any) > 0) $rows_sorted[] = $any[0];
                }
            }
        }
    }

    $rows = $rows_sorted;
    unset($rows_sorted, $partYears, $partYearAgg, $partCustSet, $partList);

    /* ---------------- COMPUTE OPEN ORDERS COUNTS ---------------- */
    $matchingOpenOrders = 0;
    $totalOpenOrders    = 0;

    // 1. Total open orders in date range (no part/customer filter)
    if ($ooDateCol !== '') {
        $totalSql = "
            SELECT COUNT(*) AS total
            FROM open_orders oo
            WHERE oo.$ooDateCol BETWEEN '$start_date' AND '$end_date'
        ";
        $totalRes = mysqli_query($connection, $totalSql);
        if ($totalRes) {
            $totalRow = mysqli_fetch_assoc($totalRes);
            $totalOpenOrders = (int)$totalRow['total'];
            mysqli_free_result($totalRes);
        }
    } else {
        $totalRes = mysqli_query($connection, "SELECT COUNT(*) AS total FROM open_orders");
        if ($totalRes) {
            $totalRow = mysqli_fetch_assoc($totalRes);
            $totalOpenOrders = (int)$totalRow['total'];
            mysqli_free_result($totalRes);
        }
    }

    // 2. Collect unique part numbers that are in the final report table
    $finalReportParts = array();
    for ($ri = 0; $ri < count($rows); $ri++) {
        $p = $rows[$ri]['part'];
        $finalReportParts[$p] = 1;
    }

    // 3. Count open orders only for parts that made the final report
    if (count($finalReportParts) > 0 && $ooPartCol !== '') {
        $safeParts = array();
        foreach (array_keys($finalReportParts) as $pp) {
            $safeParts[] = "'".mysqli_real_escape_string($connection, $pp)."'";
        }

        $matchSql = "
            SELECT COUNT(*) AS total
            FROM open_orders oo
            WHERE oo.$ooPartCol IN (".implode(',', $safeParts).")
              AND oo.$ooPartCol IS NOT NULL AND oo.$ooPartCol <> ''
        ";

        if ($ooDateCol !== '') {
            $matchSql .= " AND oo.$ooDateCol BETWEEN '$start_date' AND '$end_date' ";
        }

        if (count($customer_filter) > 0 && $ooCustCol !== '') {
            $safeCust = array();
            foreach ($customer_filter as $v) {
                $v = trim($v);
                if ($v === '') continue;
                $safeCust[] = "'".mysqli_real_escape_string($connection, $v)."'";
            }
            if (count($safeCust) > 0) {
                $matchSql .= " AND oo.$ooCustCol IN (".implode(',', $safeCust).") ";
            }
        }

        if ($part_filter !== '') {
            $partMatchSql = oh_part_filter_sql($connection, $part_filter);
            $matchSql .= str_replace('wd.', 'oo.', $partMatchSql);
        }

        $matchRes = mysqli_query($connection, $matchSql);
        if ($matchRes) {
            $matchRow = mysqli_fetch_assoc($matchRes);
            $matchingOpenOrders = (int)$matchRow['total'];
            mysqli_free_result($matchRes);
        }
    }

    /* ---------------- PREPARE LIST OF PART NUMBERS FOR COPY BUTTON ---------------- */
    $partListForCopy = '';
    if (count($finalReportParts) > 0) {
        $uniqueParts = array_keys($finalReportParts);
        sort($uniqueParts, SORT_STRING);
        $partListForCopy = implode("\n", $uniqueParts);
    }

    /* ---------------- OPEN ORDERS MONTH FLAGS (blue dot) ---------------- */
    $openByPartMonth = array();
    if ($ooPartCol !== '' && $ooDateCol !== '') {
        $ooSql = "
            SELECT
                oo.$ooPartCol AS part,
                YEAR(oo.$ooDateCol) AS yr,
                MONTH(oo.$ooDateCol) AS mo
            FROM open_orders oo
            WHERE oo.$ooDateCol BETWEEN '$start_date' AND '$end_date'
              AND oo.$ooDateCol >= '$HARD_START_DATE'
              AND oo.$ooPartCol IS NOT NULL
              AND oo.$ooPartCol <> ''
            GROUP BY oo.$ooPartCol, yr, mo
        ";
        $ooRes = mysqli_query($connection, $ooSql);
        if ($ooRes) {
            while ($r = mysqli_fetch_assoc($ooRes)) {
                $p = (string)$r['part'];
                $y = (int)$r['yr'];
                $m = (int)$r['mo'];
                if ($p !== '' && $y > 0 && $m > 0) {
                    $openByPartMonth[$p.'|'.$y.'|'.$m] = 1;
                }
            }
            mysqli_free_result($ooRes);
        }
    }
}

/* ---------------- LEAN EXISTING SET ---------------- */
$leanExisting = array();
if ($auth_ok) {
    $lr = mysqli_query($connection, "SELECT part_number FROM analysis_lean_cell_part_numbers");
    if ($lr) {
        while ($r = mysqli_fetch_row($lr)) $leanExisting[(string)$r[0]] = 1;
        mysqli_free_result($lr);
    }
}

/* ---------------- BUILD LEAN META ---------------- */
$leanMeta = array();
if ($auth_ok && is_array($rows) && count($rows) > 0) {

    $custSetByPart = array();
    $qtyByPartYear = array();

    for ($i=0; $i<count($rows); $i++) {
        $p = (string)$rows[$i]['part'];
        $y = (int)$rows[$i]['yr'];

        if (!isset($custSetByPart[$p])) $custSetByPart[$p] = array();

        if ($split_by_customer) {
            $c = (string)$rows[$i]['cust'];
            if ($c !== '') $custSetByPart[$p][$c] = 1;
        } else {
            $cl = isset($rows[$i]['custList']) ? (string)$rows[$i]['custList'] : '';
            if ($cl !== '') {
                $pieces = explode(',', $cl);
                for ($k=0; $k<count($pieces); $k++) {
                    $t = trim($pieces[$k]);
                    if ($t !== '') $custSetByPart[$p][$t] = 1;
                }
            }
        }

        if (!isset($qtyByPartYear[$p])) $qtyByPartYear[$p] = array();
        if (!isset($qtyByPartYear[$p][$y])) $qtyByPartYear[$p][$y] = 0;
        $qtyByPartYear[$p][$y] += (int)round((float)$rows[$i]['year_qty']);
    }

    foreach ($qtyByPartYear as $p => $yearsMap) {
        $years = array_keys($yearsMap);
        rsort($years, SORT_NUMERIC);

        $y1 = isset($years[0]) ? (int)$years[0] : 0;
        $y2 = isset($years[1]) ? (int)$years[1] : 0;
        $y3 = isset($years[2]) ? (int)$years[2] : 0;

        $q1 = ($y1>0) ? (int)$yearsMap[$y1] : 0;
        $q2 = ($y2>0) ? (int)$yearsMap[$y2] : 0;
        $q3 = ($y3>0) ? (int)$yearsMap[$y3] : 0;

        $custNums = array();
        if (isset($custSetByPart[$p])) {
            $custNums = array_keys($custSetByPart[$p]);
            sort($custNums, SORT_STRING);
        }

        $leanMeta[$p] = array(
            'customers' => implode(',', $custNums),
            'y1' => $y1, 'q1' => $q1,
            'y2' => $y2, 'q2' => $q2,
            'y3' => $y3, 'q3' => $q3
        );
    }
}

$is_export = ($auth_ok && isset($_GET['export']) && $_GET['export'] == '1');
if ($is_export) {
    header('Content-Type: text/csv; charset=utf-8');
    header('Content-Disposition: attachment; filename=order_history.csv');
    $out = fopen('php://output', 'w');

    $hdr = array('Part', 'Year');
    $hdr[] = $split_by_customer ? 'Customer #' : 'Customer #s';
    $hdr[] = 'Year Qty';
    $hdr[] = 'Qty Change';
    $hdr[] = 'Year Hours';
    $hdr[] = 'Hours Change';
    $hdr[] = 'Piece Hr';
    $hdr[] = 'Repeat';
    $hdr[] = 'Monthly Detail';
    fputcsv($out, $hdr);

    for ($ri=0; $ri<count($rows); $ri++) {
        $r = $rows[$ri];
        $months = $r['months'];
        usort($months, 'month_sorter');

        $md = array();
        for ($i=0; $i<count($months); $i++) {
            $m = $months[$i];
            $md[] = monthAbbr($m['mo']).'-'.substr((string)$m['yr'],2,2)
                .' Q'.(int)$m['qty']
                .' H'.rtrim(rtrim(number_format((float)$m['hrs'],2),'0'),'.');
        }

        $ph2 = isset($r['piece_hrs']) ? (float)$r['piece_hrs'] : 0.0;

        $line = array($r['part'], $r['yr']);
        $line[] = $split_by_customer ? $r['cust'] : (isset($r['custList']) ? $r['custList'] : '');
        $line[] = (int)$r['year_qty'];
        $line[] = $r['qty_trend'];
        $line[] = rtrim(rtrim(number_format((float)$r['year_hrs'],2),'0'),'.');
        $line[] = $r['hrs_trend'];
        $line[] = rtrim(rtrim(number_format((float)$ph2,2),'0'),'.');
        $line[] = (int)$r['year_repeat_months'];
        $line[] = implode(' | ', $md);

        fputcsv($out, $line);
    }

    fclose($out);
    exit;
}

$self = basename($_SERVER['PHP_SELF']);
?>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Order History</title>
<style>
    body { font-family: Arial, sans-serif; margin:10px; font-size:12px; background:#f5f5f5; }

    .top-tabs{ display:flex; align-items:flex-end; gap:6px; margin:0 0 8px 0; padding:0; }
    .top-tab{
        display:inline-block; padding:7px 12px; font-weight:bold; font-size:12px;
        text-decoration:none; color:#1f2a33; background:#eceff1;
        border:1px solid #cfd8dc; border-bottom:none;
        border-top-left-radius:10px; border-top-right-radius:10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
    .top-tab.active{ background:#ffffff; color:#0d47a1; border-color:#b0bec5; }
    .tab-shell{ border:1px solid #cfd8dc; border-radius:10px; background:#ffffff; padding:10px; }

    .header-row{
        display:flex; align-items:center; justify-content:space-between; gap:10px;
        margin-bottom:8px; padding:8px; background:transparent;
    }
    h1 { margin:0; font-size:21px; }

    .panel { background:#fff; border:1px solid #cfd8dc; border-radius:8px; padding:8px; margin-bottom:8px; }
    .auth-msg { color:#b71c1c; font-weight:bold; }

    .header-left { display:flex; align-items:center; gap:12px; }
    .header-right { display:flex; align-items:center; gap:10px; justify-content:flex-end; }

    .open-orders-info { display:flex; align-items:center; gap:8px; font-size:12px; color:#333; }

    .status-pill{
        display:inline-flex; align-items:center; gap:6px;
        padding:3px 8px; border-radius:6px;
        font-weight:bold; font-size:11px;
        border:1px solid #f57f17;
        background:#ffe0b2; color:#7a4a00;
        box-shadow: 0 2px 0 rgba(0,0,0,0.18), 0 3px 8px rgba(0,0,0,0.10);
        white-space:nowrap;
        animation: blink 1.5s infinite;
    }
    .status-pill.ready{
        background:#c8e6c9; border-color:#66bb6a; color:#1b5e20;
        animation: none;
    }
    .status-pill.ready .status-dot{ background:#43a047; }
    .status-dot{ width:9px; height:9px; border-radius:99px; background:#fb8c00; box-shadow: inset 0 -1px 0 rgba(0,0,0,0.25); }

    @keyframes blink {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }

    .btn {
        padding: 2px 10px;
        font-size: 11px;
        font-weight: bold;
        border-radius: 7px;
        border: 1px solid #0d47a1;
        background: linear-gradient(to bottom, #4a90e2 0%, #1565c0 100%);
        color: #fff;
        cursor: pointer;
        line-height: 1.0;
        height: 22px;
        white-space:nowrap;
        box-shadow: 0 2px 0 rgba(0,0,0,0.25), 0 3px 8px rgba(0,0,0,0.12);
    }
    .btn:hover { filter: brightness(1.03); }
    .btn:active { transform: translateY(1px); box-shadow: 0 1px 0 rgba(0,0,0,0.25), 0 2px 6px rgba(0,0,0,0.12); }
    .btn.green { border-color:#1b5e20; background: linear-gradient(to bottom, #66bb6a 0%, #2e7d32 100%); }

    .filters { display:flex; flex-direction:column; gap:6px; font-size:11px; }
    .filters label { font-weight:bold; margin-right:4px; }
    .filters input, .filters select { font-size:11px; padding:2px 4px; height:22px; box-sizing:border-box; }

    .narrow33 { width:36px; }
    .preset50 { width:80px; }
    .cust-select { width:190px; height:22px; line-height:18px; }
    .part-filter { width:120px; }
    .sort-select { width:120px; }

    .filters-row { display:flex; flex-wrap:wrap; align-items:center; gap:10px; width:100%; }
    .right-pack { margin-left:auto; display:flex; align-items:center; gap:10px; flex-wrap:nowrap; justify-content:flex-end; }

    .cb-wrap{ display:flex; align-items:center; gap:6px; height:22px; line-height:22px; white-space:nowrap; }
    .cb-wrap input[type="checkbox"]{ width:16px; height:22px; margin:0; vertical-align:middle; }
    .cb-wrap span{ font-weight:bold; display:inline-block; line-height:22px; }

    .lean-controls { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
    .lean-notes { width:220px; }
    .lean-status { display:inline-block; min-width:320px; padding-left:8px; font-size:11px; color:#546e7a; white-space:nowrap; }

    .month-scrollbar{
        border:1px solid #ccc; border-radius:6px; background:#fff;
        overflow-x:scroll; overflow-y:hidden; height:18px; margin-bottom:6px;
        scrollbar-width: thin;
    }
    .month-scrollbar::-webkit-scrollbar {
        height: 6px;
    }
    .month-scrollbar::-webkit-scrollbar-track {
        background: #f1f1f1;
    }
    .month-scrollbar::-webkit-scrollbar-thumb {
        background: #888;
    }
    .month-scrollbar::-webkit-scrollbar-thumb:hover {
        background: #555;
    }
    .month-scrollbar-inner{ height:1px; }

    .table-container { border:1px solid #ccc; border-radius:6px; overflow-y:auto; overflow-x:hidden; max-height:648px; background:#fff; position:relative; }

    table.report{ border-collapse:separate; border-spacing:0; table-layout:fixed; width:100%; min-width:100%; }
    table.report th, table.report td{
        border-right:1px solid #ccc; border-bottom:1px solid #ccc; border-top:0; border-left:0;
        padding:2px 4px; font-size:11px; line-height:1.0; white-space:nowrap; overflow:hidden;
        background-clip:padding-box; background-color:#fff; vertical-align:middle;
    }
    table.report thead th { border-top:1px solid #ccc; }
    table.report tr > *:first-child { border-left:1px solid #ccc; }

    table.report thead th{
        position:sticky; top:0; z-index:100;
        background:#e0f2f1; background-color:#e0f2f1;
        box-shadow: inset 0 -2px 0 #263238;
        font-size:12px; padding:4px 6px; line-height:1.1; height:26px;
    }

    tr.part-first { background:#d9eefc !important; }
    tr.part-first td { background:#d9eefc !important; }
    tr.part-gap td { border:none !important; height:16px; background:transparent; padding:0; }

    .right { text-align:right; }
    .center { text-align:center; }

    th.col-part,  td.col-part  { width:193px; min-width:193px; max-width:193px; }
    th.col-year,  td.col-year  { width:45px;  min-width:45px;  max-width:45px;  }
    th.col-cust,  td.col-cust  { width:91px;  min-width:91px;  max-width:91px;  }
    th.col-yqty,  td.col-yqty  { width:63px;  min-width:63px;  max-width:63px;  }
    th.col-qdel,  td.col-qdel  { width:50px;  min-width:50px;  max-width:50px;  }
    th.col-yhrs,  td.col-yhrs  { width:70px;  min-width:70px;  max-width:70px;  }
    th.col-hdel,  td.col-hdel  { width:50px;  min-width:50px;  max-width:50px;  }
    th.col-ph,    td.col-ph    { width:62px;  min-width:62px;  max-width:62px;  }
    th.col-rep,   td.col-rep   { width:65px;  min-width:65px;  max-width:65px;  }
    th.col-month, td.col-month { width:auto; }

    td.col-part, th.col-part,
    td.col-year, th.col-year,
    td.col-cust, th.col-cust,
    td.col-yqty, th.col-yqty,
    td.col-qdel, th.col-qdel,
    td.col-yhrs, th.col-yhrs,
    td.col-hdel, th.col-hdel,
    td.col-ph,   th.col-ph,
    td.col-rep,  th.col-rep{ position:sticky; background-clip:padding-box; }

    td.col-part, th.col-part { left:0px; }
    td.col-year, th.col-year { left:193px; }
    td.col-cust, th.col-cust { left:238px; }
    td.col-yqty, th.col-yqty { left:329px; }
    td.col-qdel, th.col-qdel { left:392px; }
    td.col-yhrs, th.col-yhrs { left:442px; }
    td.col-hdel, th.col-hdel { left:512px; }
    td.col-ph,   th.col-ph   { left:562px; }
    td.col-rep,  th.col-rep  { left:624px; }

    table.report tbody td.col-part,
    table.report tbody td.col-year,
    table.report tbody td.col-cust,
    table.report tbody td.col-yqty,
    table.report tbody td.col-qdel,
    table.report tbody td.col-yhrs,
    table.report tbody td.col-hdel,
    table.report tbody td.col-ph,
    table.report tbody td.col-rep { z-index:30; background:#fff; }

    tr.part-first td.col-part, tr.part-first td.col-year, tr.part-first td.col-cust,
    tr.part-first td.col-yqty, tr.part-first td.col-qdel, tr.part-first td.col-yhrs,
    tr.part-first td.col-hdel, tr.part-first td.col-ph, tr.part-first td.col-rep { background:#d9eefc !important; }

    table.report thead th.col-part,
    table.report thead th.col-year,
    table.report thead th.col-cust,
    table.report thead th.col-yqty,
    table.report thead th.col-qdel,
    table.report thead th.col-yhrs,
    table.report thead th.col-hdel,
    table.report thead th.col-ph,
    table.report thead th.col-rep { z-index:120; }

    td.col-rep, th.col-rep { border-right:1px solid #888 !important; box-shadow: 1px 0 0 #888; }

    td.col-cust, td.col-yqty, td.col-yhrs, td.col-ph, td.col-rep{ text-align:right !important; padding-right:10px !important; }

    .part-wrap { display:flex; align-items:center; gap:6px; width:100%; }
    .part-text { flex:1 1 auto; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .part-mid  { flex:0 0 18px; width:18px; text-align:center; }
    .part-flag { flex:0 0 16px; width:16px; text-align:center; }

    .lean-flag { display:inline-block; width:14px; height:14px; line-height:14px; text-align:center; font-size:10px; font-weight:bold; border-radius:4px; background:#FFFFCC; border:1px solid #d4b100; color:#6b5a00; }

    td.col-month { padding:0; }

    .month-viewport{
        width:100%;
        overflow-x:auto;
        overflow-y:hidden;
        padding:2px 4px;
        box-sizing:border-box;
        scrollbar-width: none;
        -ms-overflow-style: none;
    }
    .month-viewport::-webkit-scrollbar {
        display: none;
    }

    .month-inner{
        display:inline-block;
        white-space:nowrap;
        box-sizing:border-box;
        width:auto;
    }

    .month-table{ border-collapse:collapse; table-layout:auto; width:max-content; }
    .month-table td{
        border:1px solid #cfd8dc !important;
        font-size:10px; color:#263238; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
        padding:0 5px; height:12px; line-height:12px; box-sizing:border-box; background:#fff;
    }
    .month-table td.mo { background:#FFFFCC !important; }

    .month-table td.mo  { width:66px; font-weight:bold; color:#37474f; }
    .month-table td.qty { width:71px;  text-align:right; padding-right:10px; background:#fff !important; }
    .month-table td.hrs { width:90px;  text-align:right; padding-right:10px; background:#fff !important; }

    .month-table td.gap{
        width:30px !important; min-width:30px !important; max-width:30px !important;
        padding:0 !important; border:none !important; background:transparent !important;
    }

    .mo-wrap{ display:flex; align-items:center; justify-content:space-between; gap:6px; width:100%; }
    .open-dot{
        width:7px; height:7px; border-radius:99px;
        background:#1e88e5;
        box-shadow: inset 0 -1px 0 rgba(0,0,0,0.25);
        flex:0 0 7px;
    }

    /* Fixed header styling - normal black text */
    .trend {
        width:68px;
        text-align:center;
        font-weight:bold;
        color:red;
        font-family:tahoma;
        font-size:22px !important;
        vertical-align: middle !important;
        padding-bottom:2px !important;
    }

    .trend_header {
        width:68px;
        text-align:center;
        font-weight:bold;
        color:#000;
        font-size:12px !important;
    }

    .customer-row {
        margin: 8px 0;
        padding: 8px;
        background: #f9f9f9;
        border: 1px solid #ddd;
        border-radius: 4px;
        overflow-x: auto;
        scrollbar-width: thin;
        font-size: 11px;
    }
    .customer-row::-webkit-scrollbar {
        height: 6px;
    }
    .customer-row::-webkit-scrollbar-track {
        background: #f1f1f1;
    }
    .customer-row::-webkit-scrollbar-thumb {
        background: #888;
    }
    .customer-row::-webkit-scrollbar-thumb:hover {
        background: #555;
    }
    .customer-item {
        display: inline-block;
        padding: 4px 8px;
        margin-right: 8px;
        background: #fff;
        border: 1px solid #ccc;
        border-radius: 4px;
        white-space: nowrap;
        min-width: 180px;
        text-align: left;
        font-size: 11px;
    }

    .loaded-msg {
        font-size: 12px;
        color: #333;
        margin-left: 10px;
    }
</style>

</head>
<body>

<script>
(function(){
    function setProcessing(){
        var pill = document.getElementById('report_status');
        var txt  = document.getElementById('report_status_text');
        if (!pill || !txt) return;
        pill.classList.remove('ready');
        pill.classList.add('processing');
        txt.textContent = 'Processing';
    }

    function setReady(){
        var pill = document.getElementById('report_status');
        var txt  = document.getElementById('report_status_text');
        if (!pill || !txt) return;
        pill.classList.remove('processing');
        pill.classList.add('ready');
        txt.textContent = 'Report Ready';
    }

    setProcessing();

    var form = document.getElementById('filtersForm');
    if (form) {
        form.addEventListener('submit', setProcessing);
    }

    window.addEventListener('load', function(){
        setReady();
    });
})();
</script>

<div class="top-tabs">
    <a class="top-tab" href="home.php">Home</a>
    <a class="top-tab active" href="<?php echo h($self); ?>">Order Report</a>
    <a class="top-tab" href="analysis_low_vol_quick_turn_lean_report.php">Lean Report</a>
</div>

<div class="tab-shell">
    <div class="header-row">
        <div class="header-left">
            <h1>Order History</h1>
            <?php if(!$auth_ok): ?>
                <div class="auth-msg"><?php echo h($auth_msg !== '' ? $auth_msg : 'You are not authorized to run this report.'); ?></div>
            <?php endif; ?>
        </div>
        <div class="header-right">
            <?php if($auth_ok): ?>
                <div class="open-orders-info">
                    <button class="btn" id="copyPartsBtn" title="Copy all part numbers from this report to clipboard">Copy Parts</button>
                    <?php echo $matchingOpenOrders; ?> matching open orders out of <?php echo $totalOpenOrders; ?> total
                </div>
                <button class="btn" id="toggle_customers" type="button">Show Customers</button>
                <span id="report_status" class="status-pill processing">
                    <span class="status-dot"></span>
                    <span id="report_status_text">Processing</span>
                </span>
                <?php if($active_only): ?>
                    <span style="color:#1e88e5;font-size:12px;margin-left:10px;">(showing only parts with open orders)</span>
                <?php endif; ?>
                <div class="loaded-msg">
                    Loaded (<?php echo (int)$load_ms; ?>ms)
                </div>
                <button class="btn" type="button" onclick="exportCsv()">CSV</button>
            <?php endif; ?>
        </div>
    </div>

    <!-- Hidden textarea for part list (used by clipboard API) -->
    <textarea id="partListTextarea" style="position:absolute; left:-9999px;" readonly><?php echo h($partListForCopy); ?></textarea>

    <div id="customer_row" class="customer-row" style="display:none;">
        <div style="white-space:nowrap;">
            <?php foreach ($customerOptions as $cn): ?>
                <div class="customer-item">
                    <strong><?php echo h($cn); ?></strong>: 
                    <?php echo h(isset($customerData[$cn]) ? $customerData[$cn] : 'Unknown'); ?>
                </div>
            <?php endforeach; ?>
        </div>
    </div>

    <?php if(!$auth_ok): ?>
    <form method="post" class="panel">
        <div class="filters">
            <div>
                <label>User</label>
                <select name="user_emp">
                    <option value="">-- Select --</option>
                    <?php foreach($users as $u): ?>
                        <option value="<?php echo h($u['employeeNumber']); ?>"><?php echo h($u['firstName'].' '.$u['lastName']); ?></option>
                    <?php endforeach; ?>
                </select>
            </div>
            <div>
                <label>Password</label>
                <input type="password" name="user_pass" autocomplete="off">
            </div>
            <button class="btn green" name="action" value="login">Authorize</button>
        </div>
    </form>
    </div></body></html>
    <?php exit; endif; ?>

    <form method="post" class="panel" id="filtersForm">
        <div class="filters">
            <div class="filters-row">
                <div>
                    <label>Years Back</label>
                    <select name="years_back">
                        <?php for($i=1;$i<=12;$i++): ?>
                            <option value="<?php echo $i; ?>" <?php if($years_back==$i) echo 'selected="selected"'; ?>><?php echo $i; ?></option>
                        <?php endfor; ?>
                    </select>
                </div>

                <div>
                    <label>Customers</label>
                    <select name="customer_filter[]" class="cust-select" multiple="multiple" size="1" id="customer_filter"
                            title="Hold Ctrl/Shift to select multiple">
                        <option value="__ALL__">-- Select All --</option>
                        <option value="__NONE__">-- Deselect All --</option>
                        <?php
                        for ($i=0; $i<count($customerOptions); $i++) {
                            $cn = $customerOptions[$i];
                            $sel = in_array($cn, $customer_filter) ? 'selected="selected"' : '';
                            echo '<option value="'.h($cn).'" '.$sel.'>'.h($cn).'</option>';
                        }
                        ?>
                    </select>
                </div>

                <div><label>Min Hr</label><input type="text" name="min_hours" value="<?php echo h($min_hours); ?>" class="narrow33"></div>
                <div><label>Max Hr</label><input type="text" name="max_hours" value="<?php echo h($max_hours); ?>" class="narrow33"></div>
                <div><label>Min PieceHr</label><input type="text" name="min_piece_hours" value="<?php echo h($min_piece_hours); ?>" class="narrow33"></div>
                <div><label>Max PieceHr</label><input type="text" name="max_piece_hours" value="<?php echo h($max_piece_hours); ?>" class="narrow33"></div>
                <div><label>Min Rep</label><input type="text" name="min_repeats" value="<?php echo h($min_repeats); ?>" class="narrow33" inputmode="numeric" pattern="[0-9]*"></div>
                <div><label>Max Rep</label><input type="text" name="max_repeats" value="<?php echo h($max_repeats); ?>" class="narrow33" inputmode="numeric" pattern="[0-9]*"></div>

                <div><label>Part #</label><input type="text" name="part_filter" value="<?php echo h($part_filter); ?>" class="part-filter" placeholder="ex: 1234 or 12*"></div>

                <div class="cb-wrap">
                    <span>Only Repeats</span>
                    <input type="checkbox" name="only_repeats" value="1" <?php if($only_repeats) echo 'checked="checked"'; ?>>
                </div>

                <div class="cb-wrap">
                    <span>Split by Customer</span>
                    <input type="checkbox" name="split_by_customer" value="1" <?php if($split_by_customer) echo 'checked="checked"'; ?>>
                </div>

                <div class="cb-wrap">
                    <span>Active Orders Only</span>
                    <input type="checkbox" name="active_only" value="1" <?php if($active_only) echo 'checked="checked"'; ?>>
                </div>

                <div>
                    <label>Min Open Qty</label>
                    <input type="text" name="min_open_qty" value="<?php echo h($min_open_qty); ?>" class="narrow33" inputmode="numeric" pattern="[0-9]*">
                </div>

                <div>
                    <label>Max Open Qty</label>
                    <input type="text" name="max_open_qty" value="<?php echo h($max_open_qty); ?>" class="narrow33" inputmode="numeric" pattern="[0-9]*">
                </div>

                <button class="btn green" type="submit" style="margin-left:auto;">Run</button>
            </div>

            <div class="filters-row" style="flex-wrap:nowrap;">
                <div style="display:flex; align-items:center; gap:8px; flex-wrap:nowrap;">
                    <label>Preset</label>

                    <select name="preset_name" style="width:160px;">
                        <option value="">-- select --</option>
                        <?php foreach ($_SESSION['oh_presets'] as $k=>$v): ?>
                            <option value="<?php echo h($k); ?>"><?php echo h($k); ?></option>
                        <?php endforeach; ?>
                    </select>

                    <input type="text" name="preset_name_new" value="" placeholder="Preset name" class="preset50">

                    <button class="btn green" type="submit" name="preset_action" value="save"
                            onclick="document.getElementsByName('preset_name')[0].value=document.getElementsByName('preset_name_new')[0].value;">
                        Save
                    </button>

                    <button class="btn" type="submit" name="preset_action" value="load">Load</button>
                    <button class="btn" type="submit" name="preset_action" value="delete">Delete</button>

                    <div class="lean-controls" style="margin-left:10px;">
                        <input type="text" id="lean_notes" class="lean-notes" maxlength="300" placeholder="Notes (optional, applies to add/update)">
                        <button class="btn" type="button" onclick="leanSelectAll()">Select All</button>
                        <button class="btn" type="button" onclick="leanDeselectAll()">Deselect All</button>
                        <button class="btn green" type="button" onclick="addSelectedToLean()">Add to Lean</button>
                        <span id="lean_status" class="lean-status"></span>
                    </div>
                </div>

                <div class="right-pack" style="flex-shrink:0;">
                    <div>
                        <label>User</label>
                        <select name="user_emp" style="width:120px;">
                            <option value="">-- Select --</option>
                            <?php foreach($users as $u): ?>
                                <option value="<?php echo h($u['employeeNumber']); ?>" <?php if(isset($_SESSION['oh_emp']) && (string)$_SESSION['oh_emp'] === (string)$u['employeeNumber']) echo 'selected="selected"'; ?>>
                                    <?php echo h($u['firstName'].' '.$u['lastName']); ?>
                                </option>
                            <?php endforeach; ?>
                        </select>
                    </div>

                    <div>
                        <label>Password</label>
                        <input type="password" name="user_pass" autocomplete="off" style="width:94px;">
                    </div>

                    <button class="btn" type="submit" name="action" value="login">Authorize</button>
                    <button class="btn" type="submit" name="action" value="logout">Logout</button>

                    <label style="font-weight:bold;">Sort</label>
                    <select name="sort_by" class="sort-select">
                        <option value="part_asc" <?php if($sort_by==='part_asc') echo 'selected="selected"'; ?>>Part # Asc</option>
                        <option value="part_desc" <?php if($sort_by==='part_desc') echo 'selected="selected"'; ?>>Part # Desc</option>
                        <option value="qty_desc" <?php if($sort_by==='qty_desc') echo 'selected="selected"'; ?>>Year Qty Desc</option>
                        <option value="qty_asc"  <?php if($sort_by==='qty_asc')  echo 'selected="selected"'; ?>>Year Qty Asc</option>
                        <option value="hrs_desc" <?php if($sort_by==='hrs_desc') echo 'selected="selected"'; ?>>Year Hours Desc</option>
                        <option value="hrs_asc"  <?php if($sort_by==='hrs_asc')  echo 'selected="selected"'; ?>>Year Hours Asc</option>
                        <option value="rep_desc" <?php if($sort_by==='rep_desc') echo 'selected="selected"'; ?>>Repeat Desc</option>
                        <option value="rep_asc"  <?php if($sort_by==='rep_asc')  echo 'selected="selected"'; ?>>Repeat Asc</option>
                        <option value="cust_desc" <?php if($sort_by==='cust_desc') echo 'selected="selected"'; ?>>Customer # Desc</option>
                        <option value="cust_asc"  <?php if($sort_by==='cust_asc')  echo 'selected="selected"'; ?>>Customer # Asc</option>
                    </select>

                    <label style="font-weight:bold;">Year</label>
                    <select name="sort_year" style="width:84px;">
                        <?php
                        $minY  = (int)substr($HARD_START_DATE, 0, 4);
                        $maxY = $maxDataYear;
                        for ($y=$maxY; $y>=$minY; $y--) {
                            $sel = ((int)$sort_year === (int)$y) ? 'selected="selected"' : '';
                            echo '<option value="'.(int)$y.'" '.$sel.'>'.(int)$y.'</option>';
                        }
                        ?>
                    </select>
                </div>
            </div>
        </div>
    </form>

    <div class="month-scrollbar" id="monthScrollTop">
        <div class="month-scrollbar-inner" id="monthScrollTopInner"></div>
    </div>

    <div class="table-container" id="tableWrap">
        <table class="report" id="reportTable">
            <colgroup>
                <col style="width:193px">
                <col style="width:45px">
                <col style="width:91px">
                <col style="width:63px">
                <col style="width:50px">
                <col style="width:70px">
                <col style="width:50px">
                <col style="width:62px">
                <col style="width:65px">
                <col>
            </colgroup>

            <thead>
                <tr>
                    <th class="col-part">Part #</th>
                    <th class="col-year center">Year</th>
                    <?php if($split_by_customer): ?>
                        <th class="col-cust right">Customer #</th>
                    <?php else: ?>
                        <th class="col-cust right">Customer #s</th>
                    <?php endif; ?>
                    <th class="right col-yqty">Year Qty</th>
                    <th class="trend_header col-qdel">Qty Δ</th>
                    <th class="right col-yhrs">Year Hours</th>
                    <th class="trend_header col-hdel">Hour Δ</th>
                    <th class="right col-ph">Piece Hr</th>
                    <th class="right col-rep">Repeat</th>
                    <th class="col-month">Monthly Summary</th>
                </tr>
            </thead>
            <tbody>
            <?php
            $lastPart = '';
            if (!is_array($rows) || count($rows) === 0) {
                echo '<tr><td colspan="10" style="padding:10px;color:#b71c1c;font-weight:bold;">No results found for current filters.</td></tr>';
            } else {
                for ($ri=0; $ri<count($rows); $ri++) {
                    $r = $rows[$ri];

                    if ($lastPart !== '' && $lastPart !== $r['part']) {
                        echo '<tr class="part-gap"><td colspan="10"></td></tr>';
                    }

                    $isFirstRowForPart = ($lastPart !== $r['part']);
                    $trClass = $isFirstRowForPart ? 'part-first' : '';
                    echo '<tr class="'.$trClass.'">';

                    $p = (string)$r['part'];
                    $already = isset($leanExisting[$p]) ? 1 : 0;

                    echo '<td class="col-part">';
                    echo '<div class="part-wrap">';
                    echo '<span class="part-text">'.h($p).'</span>';

                    echo '<span class="part-flag">';
                    if ($isFirstRowForPart && $already) echo '<span class="lean-flag">L</span>';
                    echo '</span>';

                    echo '<span class="part-mid">';
                    if ($isFirstRowForPart && !$already) {
                        $meta = isset($leanMeta[$p]) ? $leanMeta[$p] : array('customers'=>'','y1'=>0,'q1'=>0,'y2'=>0,'q2'=>0,'y3'=>0,'q3'=>0);
                        echo '<input type="checkbox" class="lean-cb"'
                           . ' data-part="'.h($p).'"'
                           . ' data-customers="'.h($meta['customers']).'"'
                           . ' data-y1="'.(int)$meta['y1'].'" data-q1="'.(int)$meta['q1'].'"' 
                           . ' data-y2="'.(int)$meta['y2'].'" data-q2="'.(int)$meta['q2'].'"' 
                           . ' data-y3="'.(int)$meta['y3'].'" data-q3="'.(int)$meta['q3'].'"'
                           . '>';
                    }
                    echo '</span>';

                    echo '</div>';
                    echo '</td>';

                    echo '<td class="col-year center">'.h($r['yr']).'</td>';

                    if ($split_by_customer) echo '<td class="col-cust">'.h($r['cust']).'</td>';
                    else echo '<td class="col-cust">'.h(isset($r['custList']) ? $r['custList'] : '').'</td>';

                    echo '<td class="col-yqty">'.number_format((float)$r['year_qty'], 0).'</td>';
                    echo '<td class="trend col-qdel">'.h($r['qty_trend']).'</td>';

                    echo '<td class="col-yhrs">'.number_format((float)$r['year_hrs'], 0).'</td>';
                    echo '<td class="trend col-hdel" style="padding-bottom:2px !important; vertical-align:middle">'.h($r['hrs_trend']).'</td>';

                    $ph = isset($r['piece_hrs']) ? (float)$r['piece_hrs'] : 0.0;
                    echo '<td class="col-ph">'.h(number_format($ph, 2)).'</td>';

                    echo '<td class="col-rep">'.(int)$r['year_repeat_months'].'</td>';

                    $months = $r['months'];
                    usort($months, 'month_sorter');

                    echo '<td class="col-month">';
                    echo '<div class="month-viewport"><div class="month-inner">';

                    if (is_array($months) && count($months) > 0) {
                        echo '<table class="month-table"><tr>';
                        for ($i=0; $i<count($months); $i++) {
                            $m = $months[$i];
                            $abbr = monthAbbr($m['mo']);
                            $yy = substr((string)$m['yr'], 2, 2);

                            $qtyI = (int)$m['qty'];
                            $hrsF = (float)$m['hrs'];
                            $hrsTxt = rtrim(rtrim(number_format($hrsF, 2), '0'), '.');

                            $openKey = $p.'|'.(int)$m['yr'].'|'.(int)$m['mo'];
                            $isOpen = isset($openByPartMonth[$openKey]) ? 1 : 0;

                            echo '<td class="mo"><span class="mo-wrap"><span>'.h($abbr.'-'.$yy).'</span>'.($isOpen ? '<span class="open-dot"></span>' : '<span style="width:7px;height:7px;"></span>').'</span></td>';
                            echo '<td class="qty">'.h($qtyI.' pcs').'</td>';
                            echo '<td class="hrs">'.h($hrsTxt.' hrs').'</td>';

                            if ($i < count($months)-1) echo '<td class="gap"></td>';
                        }
                        echo '</tr></table>';
                    }

                    echo '</div></div>';
                    echo '</td>';

                    echo '</tr>';
                    $lastPart = $r['part'];
                }
            }
            ?>
            </tbody>
        </table>
    </div>

    <div class="month-scrollbar" id="monthScrollBottom" style="margin-top:6px;">
        <div class="month-scrollbar-inner" id="monthScrollBottomInner"></div>
    </div>

</div>

<script>
/* Copy Parts button functionality */
document.getElementById('copyPartsBtn').addEventListener('click', function() {
    var textarea = document.getElementById('partListTextarea');
    textarea.select();
    textarea.setSelectionRange(0, 99999); /* For mobile devices */

    navigator.clipboard.writeText(textarea.value).then(function() {
        alert('Part numbers copied to clipboard!\n\n' + textarea.value);
    }, function(err) {
        alert('Could not copy part numbers. Please select and copy manually.');
    });
});

/* all your existing scripts (lean, scroll sync, etc.) remain unchanged */
(function(){
    var sel = document.getElementById('customer_filter');
    if (!sel) return;

    sel.addEventListener('change', function(){
        var allOpt  = sel.options[0];
        var noneOpt = sel.options[1];

        if (allOpt.selected) {
            for (var i=2; i<sel.options.length; i++) sel.options[i].selected = true;
            allOpt.selected = false; noneOpt.selected = false;
            return;
        }
        if (noneOpt.selected) {
            for (var j=2; j<sel.options.length; j++) sel.options[j].selected = false;
            allOpt.selected = false; noneOpt.selected = false;
            return;
        }
    });
})();

function exportCsv() {
    var f = document.createElement('form');
    f.method = 'post';
    f.action = window.location.pathname + '?export=1';

    var src = document.getElementById('filtersForm');
    if (src) {
        var els = src.querySelectorAll('input,select');
        for (var i=0;i<els.length;i++){
            var e = els[i];
            if (!e.name) continue;

            if (e.tagName.toLowerCase() === 'select' && e.multiple) {
                for (var j=0;j<e.options.length;j++){
                    if (!e.options[j].selected) continue;
                    if (e.options[j].value === '__ALL__' || e.options[j].value === '__NONE__') continue;
                    var inpM = document.createElement('input');
                    inpM.type = 'hidden';
                    inpM.name = e.name;
                    inpM.value = e.options[j].value;
                    f.appendChild(inpM);
                }
                continue;
            }

            if (e.type === 'checkbox') {
                if (!e.checked) continue;
                var inp = document.createElement('input');
                inp.type = 'hidden';
                inp.name = e.name;
                inp.value = e.value ? e.value : '1';
                f.appendChild(inp);
            } else {
                if (e.name === 'preset_name_new') continue;
                if ((e.name === 'user_pass') && (!e.value || e.value.trim()==='')) continue;

                var inp2 = document.createElement('input');
                inp2.type = 'hidden';
                inp2.name = e.name;
                inp2.value = e.value;
                f.appendChild(inp2);
            }
        }
    }
    document.body.appendChild(f);
    f.submit();
}

function setLeanStatus(msg, ok){
    var el = document.getElementById('lean_status');
    if (!el) return;
    el.textContent = msg || '';
    el.style.color = ok ? '#2e7d32' : '#b71c1c';
}

function leanSelectAll(){
    var cbs = document.querySelectorAll('.lean-cb');
    for (var i=0;i<cbs.length;i++){
        if (!cbs[i].disabled) cbs[i].checked = true;
    }
    setLeanStatus('Selected all visible parts.', true);
}

function leanDeselectAll(){
    var cbs = document.querySelectorAll('.lean-cb');
    for (var i=0;i<cbs.length;i++){
        cbs[i].checked = false;
    }
    setLeanStatus('Deselected all.', true);
}

function addSelectedToLean(){
    var cbs = document.querySelectorAll('.lean-cb:checked');
    if (!cbs || cbs.length === 0){
        setLeanStatus('No parts selected.', false);
        return;
    }

    var notesEl = document.getElementById('lean_notes');
    var notes = notesEl ? (notesEl.value || '').trim() : '';

    var parts = [];
    for (var i=0;i<cbs.length;i++){
        var cb = cbs[i];
        parts.push({
            part: cb.getAttribute('data-part') || '',
            customers: cb.getAttribute('data-customers') || '',
            y1: parseInt(cb.getAttribute('data-y1') || '0', 10) || 0,
            q1: parseInt(cb.getAttribute('data-q1') || '0', 10) || 0,
            y2: parseInt(cb.getAttribute('data-y2') || '0', 10) || 0,
            q2: parseInt(cb.getAttribute('data-q2') || '0', 10) || 0,
            y3: parseInt(cb.getAttribute('data-y3') || '0', 10) || 0,
            q3: parseInt(cb.getAttribute('data-q3') || '0', 10) || 0
        });
    }

    setLeanStatus('Saving to Lean…', true);

    var fd = new FormData();
    fd.append('action', 'lean_add');
    fd.append('payload', JSON.stringify({ notes: notes, parts: parts }));

    fetch(window.location.pathname, {
        method: 'POST',
        credentials: 'same-origin',
        body: fd
    })
    .then(function(r){ return r.json(); })
    .then(function(j){
        if (!j || !j.ok){
            setLeanStatus((j && j.msg) ? j.msg : 'Lean save failed.', false);
            return;
        }
        setLeanStatus(j.msg + ' Added ' + (j.added||0) + ' Updated ' + (j.updated||0) + ' Skipped ' + (j.skipped||0), true);

        for (var i=0;i<cbs.length;i++){
            var cb = cbs[i];
            cb.checked = false;

            var tr = cb.closest('tr');
            if (!tr) continue;

            var flagCell = tr.querySelector('.part-flag');
            if (flagCell && !flagCell.querySelector('.lean-flag')) {
                var sp = document.createElement('span');
                sp.className = 'lean-flag';
                sp.textContent = 'L';
                flagCell.appendChild(sp);
            }

            cb.disabled = true;
        }
    })
    .catch(function(){
        setLeanStatus('Lean save failed (network/server).', false);
    });
}

/* ---------- Monthly scroll sync ---------- */
(function(){
    var top = document.getElementById('monthScrollTop');
    var topInner = document.getElementById('monthScrollTopInner');
    var bot = document.getElementById('monthScrollBottom');
    var botInner = document.getElementById('monthScrollBottomInner');

    if (!top || !topInner || !bot || !botInner) return;

    function viewports(){ return document.querySelectorAll('.month-viewport'); }
    function monthInners(){ return document.querySelectorAll('.month-inner'); }
    function monthTables(){ return document.querySelectorAll('.month-table'); }

    function clearForcedWidths(){
        var inn = monthInners();
        for (var i=0;i<inn.length;i++){
            inn[i].style.width = '';
            inn[i].style.minWidth = '';
        }
        topInner.style.width = '';
        botInner.style.width = '';
    }

    function computeGlobalMonthWidth(){
        clearForcedWidths();

        var maxW = 0;

        var tbls = monthTables();
        for (var t=0; t<tbls.length; t++){
            var w1 = tbls[t].scrollWidth || 0;
            if (w1 > maxW) maxW = w1;
        }

        var vps = viewports();
        for (var v=0; v<vps.length; v++){
            var w2 = vps[v].scrollWidth || 0;
            if (w2 > maxW) maxW = w2;
        }

        var vw = top.clientWidth || 0;
        if (maxW < vw) maxW = vw;

        maxW = maxW + 2600;
        if (maxW < 12000) maxW = 12000;

        return maxW;
    }

    function applyGlobalWidths(contentW){
        topInner.style.width = contentW + 'px';
        botInner.style.width = contentW + 'px';

        var inn = monthInners();
        for (var i=0;i<inn.length;i++){
            inn[i].style.width = contentW + 'px';
            inn[i].style.minWidth = contentW + 'px';
        }
    }

    function applyScrollLeft(x){
        var vps = viewports();
        for (var i=0;i<vps.length;i++){
            vps[i].scrollLeft = x;
        }
    }

    var lock = 0;

    function setGlobalX(x){
        if (lock) return;
        lock = 1;
        top.scrollLeft = x;
        bot.scrollLeft = x;
        applyScrollLeft(x);
        lock = 0;
    }

    function attachViewportGuards(){
        var vps = viewports();
        for (var i=0;i<vps.length;i++){
            (function(vp){
                vp.addEventListener('wheel', function(e){
                    var dx = e.deltaX || 0;
                    var dy = e.deltaY || 0;
                    var horiz = (Math.abs(dx) > 0.5) || e.shiftKey;
                    if (horiz) {
                        e.preventDefault();
                        var step = e.shiftKey ? dy : dx;
                        setGlobalX((top.scrollLeft||0) + step);
                    }
                }, { passive:false });

                vp.addEventListener('scroll', function(){
                    if (lock) return;
                    var gx = top.scrollLeft || 0;
                    if (Math.abs(vp.scrollLeft - gx) > 1) {
                        lock = 1;
                        vp.scrollLeft = gx;
                        lock = 0;
                    }
                });
            })(vps[i]);
        }
    }

    function init(){
        var contentW = computeGlobalMonthWidth();
        applyGlobalWidths(contentW);
        setGlobalX(top.scrollLeft || 0);
    }

    top.addEventListener('scroll', function(){
        if (lock) return;
        lock = 1;
        bot.scrollLeft = top.scrollLeft;
        applyScrollLeft(top.scrollLeft);
        lock = 0;
    });

    bot.addEventListener('scroll', function(){
        if (lock) return;
        lock = 1;
        top.scrollLeft = bot.scrollLeft;
        applyScrollLeft(bot.scrollLeft);
        lock = 0;
    });

    attachViewportGuards();

    init();
    requestAnimationFrame(init);
    setTimeout(init, 80);
    setTimeout(init, 220);
    setTimeout(init, 520);
    window.addEventListener('resize', init);
    window.addEventListener('load', init);
})();

document.addEventListener('DOMContentLoaded', function(){
    var toggleBtn = document.getElementById('toggle_customers');
    if (toggleBtn) {
        toggleBtn.onclick = function(){
            var row = document.getElementById('customer_row');
            if (row.style.display === 'none' || row.style.display === '') {
                row.style.display = 'block';
                toggleBtn.textContent = 'Hide Customers';
            } else {
                row.style.display = 'none';
                toggleBtn.textContent = 'Show Customers';
            }
        };
    }
});
</script>

</body>
</html>
