add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-summary', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_summary',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_summary($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_summary';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate', 'gameId', 'skaterFullName', 'playerId', 'homeRoad',
		'teamAbbrev', 'opponentTeamAbbrev', 'shootsCatches', 'positionCode',
		'gamesPlayed', 'goals', 'assists', 'points', 'plusMinus',
		'penaltyMinutes', 'pointsPerGame', 'evGoals', 'evPoints',
		'ppGoals', 'ppPoints', 'shGoals', 'shPoints', 'otGoals',
		'gameWinningGoals', 'shots', 'shootingPct', 'timeOnIcePerGame',
		'faceoffWinPct', 'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-summary-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_summary_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_summary_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_summary';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-summary-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_summary_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_summary_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_summary';

    $full = $request->get_param('full');

    $where = [];
    $params = [];

    if (!$full) {

        $teams_raw = sanitize_text_field($request->get_param('teams'));
		$opponents_raw = sanitize_text_field($request->get_param('opponents'));
		$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
		$positionCode = sanitize_text_field($request->get_param('positionCode'));
        $search = sanitize_text_field($request->get_param('search'));
        $date_single = sanitize_text_field($request->get_param('date_single'));
        $date_start = sanitize_text_field($request->get_param('date_start'));
        $date_end = sanitize_text_field($request->get_param('date_end'));

        if (!empty($teams_raw)) {
            $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
            if ($teams) {
                $placeholders = implode(',', array_fill(0, count($teams), '%s'));
                $where[] = "teamAbbrev IN ($placeholders)";
                foreach ($teams as $t) $params[] = $t;
            }
        }
		
		if (!empty($opponents_raw)) {
			$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
			if ($opponents) {
				$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
				$where[] = "opponentTeamAbbrev IN ($placeholders)";
				foreach ($opponents as $o) $params[] = $o;
			}
		}
		
		if (!empty($homeRoad)) {
			$where[] = "homeRoad = %s";
			$params[] = $homeRoad;
		}
		
		if (!empty($positionCode)) {
			$where[] = "positionCode = %s";
			$params[] = $positionCode;
		}

        if (!empty($search)) {
            $like = '%' . $wpdb->esc_like($search) . '%';
            $where[] = "(skaterFullName LIKE %s)";
            $params[] = $like;
        }

        if (!empty($date_single)) {
            $where[] = "gameDate = %s";
            $params[] = $date_single;
        } elseif (!empty($date_start) && !empty($date_end)) {
            $where[] = "gameDate BETWEEN %s AND %s";
            $params[] = $date_start;
            $params[] = $date_end;
        }
    }

    $where_sql = $where ? "WHERE " . implode(" AND ", $where) : "";

    $sql = "SELECT * FROM $table $where_sql ORDER BY gameDate DESC";

    $rows = $params
        ? $wpdb->get_results($wpdb->prepare($sql, ...$params), ARRAY_A)
        : $wpdb->get_results($sql, ARRAY_A);

    header('Content-Type: text/csv');

	$filename = $full
		? 'full_skater_summary.csv'
		: 'filtered_skater_summary.csv';

	header("Content-Disposition: attachment; filename={$filename}");

    $out = fopen('php://output', 'w');

    if (!empty($rows)) {
        fputcsv($out, array_keys($rows[0]));
        foreach ($rows as $row) {
            fputcsv($out, $row);
        }
    }

    fclose($out);
    exit;
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-bios', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_bios',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_bios($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_bios';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $search = sanitize_text_field($request->get_param('search'));
    $teams_raw = sanitize_text_field($request->get_param('teams'));
    $positionCode = sanitize_text_field($request->get_param('positionCode'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if ($teams) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "currentTeamAbbrev IN ($placeholders)";
            foreach ($teams as $t) $params[] = $t;
        }
    }

    if (!empty($positionCode)) {
        $where[] = "positionCode = %s";
        $params[] = $positionCode;
    }

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
        $where[] = "skaterFullName LIKE %s";
        $params[] = $like;
    }

    $where_sql = $where ? "WHERE " . implode(" AND ", $where) : "";

    $total = $params
        ? intval($wpdb->get_var($wpdb->prepare("SELECT COUNT(*) FROM $table $where_sql", ...$params)))
        : intval($wpdb->get_var("SELECT COUNT(*) FROM $table"));

    $last_page = max(1, ceil($total / $per_page));

    $sql = "SELECT *
            FROM $table
            $where_sql
            ORDER BY skaterFullName ASC
            LIMIT %d OFFSET %d";

    $rows = $wpdb->get_results(
        $wpdb->prepare($sql, ...array_merge($params, [$per_page, $offset])),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-bios-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_bios_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_bios_csv($request) {
    global $wpdb;

    $wpdb->query("SET NAMES utf8mb4");
    $wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_bios';

    $full = $request->get_param('full');

    $where = [];
    $params = [];

    if (!$full) {
        $teams_raw = sanitize_text_field($request->get_param('teams'));
        $positionCode = sanitize_text_field($request->get_param('positionCode'));
        $search = sanitize_text_field($request->get_param('search'));

        if (!empty($teams_raw)) {
            $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
            if ($teams) {
                $placeholders = implode(',', array_fill(0, count($teams), '%s'));
                $where[] = "currentTeamAbbrev IN ($placeholders)";
                foreach ($teams as $t) $params[] = $t;
            }
        }

        if (!empty($positionCode)) {
            $where[] = "positionCode = %s";
            $params[] = $positionCode;
        }

        if (!empty($search)) {
            $like = '%' . $wpdb->esc_like($search) . '%';
            $where[] = "skaterFullName LIKE %s";
            $params[] = $like;
        }
    }

    $where_sql = $where ? "WHERE " . implode(" AND ", $where) : "";

    $sql = "SELECT * FROM $table $where_sql ORDER BY skaterFullName ASC";

    $rows = $params
        ? $wpdb->get_results($wpdb->prepare($sql, ...$params), ARRAY_A)
        : $wpdb->get_results($sql, ARRAY_A);

    header('Content-Type: text/csv; charset=utf-8');
    header('Content-Encoding: UTF-8');

    $filename = $full
        ? 'full_skater_bios.csv'
        : 'filtered_skater_bios.csv';

    header("Content-Disposition: attachment; filename={$filename}");

    echo "\xEF\xBB\xBF";

    $out = fopen('php://output', 'w');

    if (!empty($rows)) {
        fputcsv($out, array_keys($rows[0]));
        foreach ($rows as $row) {
            fputcsv($out, $row);
        }
    }

    fclose($out);
    exit;
}