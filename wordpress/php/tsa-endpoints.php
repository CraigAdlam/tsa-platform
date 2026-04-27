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
	
	$allowed_sort_fields = [
		'skaterFullName',
		'playerId',
		'currentTeamAbbrev',
		'currentTeamName',
		'shootsCatches',
		'positionCode',
		'birthDate',
		'birthCity',
		'birthStateProvinceCode',
		'birthCountryCode',
		'nationalityCode',
		'height',
		'weight',
		'draftYear',
		'draftRound',
		'draftOverall',
		'firstSeasonForGameType',
		'isInHallOfFameYn',
		'gamesPlayed',
		'goals',
		'assists',
		'points',
		'lastName'
	];
	
	$sort_field = 'skaterFullName';
	$sort_dir = 'ASC';

	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

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

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'desc') {
				$sort_dir = 'DESC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

	$sql = "SELECT *
			FROM $table
			$where_sql
			$order_sql
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

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-faceoffpercentages', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_faceoffpercentages',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_faceoffpercentages($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_faceoffpercentages';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'timeOnIcePerGame',
		'totalFaceoffs',
		'evFaceoffs',
		'ppFaceoffs',
		'shFaceoffs',
		'offensiveZoneFaceoffs',
		'neutralZoneFaceoffs',
		'defensiveZoneFaceoffs',
		'faceoffWinPct',
		'evFaceoffPct',
		'ppFaceoffPct',
		'shFaceoffPct',
		'offensiveZoneFaceoffPct',
		'neutralZoneFaceoffPct',
		'defensiveZoneFaceoffPct',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-faceoffpercentages-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_faceoffpercentages_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_faceoffpercentages_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_faceoffpercentages';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-faceoffpercentages-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_faceoffpercentages_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_faceoffpercentages_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_faceoffpercentages';

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
		? 'full_skater_faceoffpercentages.csv'
		: 'filtered_skater_faceoffpercentages.csv';

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
    register_rest_route('tsa/v1', '/skater-faceoffwins', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_faceoffwins',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_faceoffwins($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_faceoffwins';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'totalFaceoffs',
		'totalFaceoffWins',
		'totalFaceoffLosses',
		'faceoffWinPct',
		'evFaceoffs',
		'evFaceoffsWon',
		'evFaceoffsLost',
		'ppFaceoffs',
		'ppFaceoffsWon',
		'ppFaceoffsLost',
		'shFaceoffs',
		'shFaceoffsWon',
		'shFaceoffsLost',
		'offensiveZoneFaceoffs',
		'offensiveZoneFaceoffWins',
		'offensiveZoneFaceoffLosses',
		'neutralZoneFaceoffs',
		'neutralZoneFaceoffWins',
		'neutralZoneFaceoffLosses',
		'defensiveZoneFaceoffs',
		'defensiveZoneFaceoffWins',
		'defensiveZoneFaceoffLosses',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-faceoffwins-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_faceoffwins_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_faceoffwins_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_faceoffwins';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-faceoffwins-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_faceoffwins_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_faceoffwins_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_faceoffwins';

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
		? 'full_skater_faceoffwins.csv'
		: 'filtered_skater_faceoffwins.csv';

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
    register_rest_route('tsa/v1', '/skater-goalsforagainst', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_goalsforagainst',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_goalsforagainst($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_goalsforagainst';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'goals',
		'assists',
		'points',
		'powerPlayTimeOnIcePerGame',
		'powerPlayGoalFor',
		'shortHandedGoalsAgainst',
		'shortHandedTimeOnIcePerGame',
		'shortHandedGoalsFor',
		'powerPlayGoalsAgainst',
		'evenStrengthTimeOnIcePerGame',
		'evenStrengthGoalsFor',
		'evenStrengthGoalsAgainst',
		'evenStrengthGoalDifference',
		'evenStrengthGoalsForPct',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-goalsforagainst-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_goalsforagainst_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_goalsforagainst_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_goalsforagainst';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-goalsforagainst-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_goalsforagainst_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_goalsforagainst_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_goalsforagainst';

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
		? 'full_skater_goalsforagainst.csv'
		: 'filtered_skater_goalsforagainst.csv';

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
    register_rest_route('tsa/v1', '/skater-penalties', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penalties',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penalties($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_penalties';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'goals',
		'assists',
		'points',
		'penaltyMinutes',
		'penaltySecondsPerGame',
		'timeOnIcePerGame',
		'penaltyMinutesPerTimeOnIce',
		'penaltiesDrawn',
		'penalties',
		'netPenalties',
		'penaltiesDrawnPer60',
		'penaltiesTakenPer60',
		'netPenaltiesPer60',
		'minorPenalties',
		'majorPenalties',
		'matchPenalties',
		'misconductPenalties',
		'gameMisconductPenalties',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-penalties-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penalties_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penalties_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_penalties';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penalties-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_penalties_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_penalties_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_penalties';

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
		? 'full_skater_penalties.csv'
		: 'filtered_skater_penalties.csv';

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
    register_rest_route('tsa/v1', '/skater-penaltykill', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penaltykill',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penaltykill($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_penaltykill';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'shGoals',
		'shAssists',
		'shPrimaryAssists',
		'shSecondaryAssists',
		'shPoints',
		'shIndividualSatFor',
		'shShots',
		'shShootingPct',
		'shGoalsPer60',
		'shPrimaryAssistsPer60',
		'shSecondaryAssistsPer60',
		'shPointsPer60',
		'shIndividualSatForPer60',
		'shShotsPer60',
		'ppGoalsAgainstPer60',
		'shTimeOnIce',
		'shTimeOnIcePerGame',
		'shTimeOnIcePctPerGame',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-penaltykill-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penaltykill_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penaltykill_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_penaltykill';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penaltykill-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_penaltykill_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_penaltykill_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_penaltykill';

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
		? 'full_skater_penaltykill.csv'
		: 'filtered_skater_penaltykill.csv';

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
    register_rest_route('tsa/v1', '/skater-penaltyshots', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penaltyshots',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penaltyshots($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_penaltyshots';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'penaltyShotAttempts',
		'penaltyShotsGoals',
		'penaltyShotsFailed',
		'penaltyShotShootingPct',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-penaltyshots-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penaltyshots_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penaltyshots_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_penaltyshots';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penaltyshots-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_penaltyshots_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_penaltyshots_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_penaltyshots';

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
		? 'full_skater_penaltyshots.csv'
		: 'filtered_skater_penaltyshots.csv';

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
    register_rest_route('tsa/v1', '/skater-percentages', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_percentages',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_percentages($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_percentages';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'timeOnIcePerGame5v5',
		'satPercentage',
		'satPercentageAhead',
		'satPercentageTied',
		'satPercentageBehind',
		'satPercentageClose',
		'satRelative',
		'usatPercentage',
		'usatPercentageAhead',
		'usatPercentageTied',
		'usatPercentageBehind',
		'usatPrecentageClose',
		'usatRelative',
		'zoneStartPct5v5',
		'shootingPct5v5',
		'skaterSavePct5v5',
		'skaterShootingPlusSavePct5v5',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-percentages-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_percentages_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_percentages_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_percentages';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-percentages-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_percentages_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_percentages_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_percentages';

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
		? 'full_skater_percentages.csv'
		: 'filtered_skater_percentages.csv';

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
    register_rest_route('tsa/v1', '/skater-powerplay', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_powerplay',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_powerplay($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_powerplay';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'ppGoals',
		'ppAssists',
		'ppPrimaryAssists',
		'ppSecondaryAssists',
		'ppPoints',
		'ppIndividualSatFor',
		'ppShots',
		'ppShootingPct',
		'ppGoalsPer60',
		'ppPrimaryAssistsPer60',
		'ppSecondaryAssistsPer60',
		'ppPointsPer60',
		'ppIndividualSatForPer60',
		'ppShotsPer60',
		'ppGoalsForPer60',
		'ppTimeOnIce',
		'ppTimeOnIcePerGame',
		'ppTimeOnIcePctPerGame',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-powerplay-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_powerplay_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_powerplay_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_powerplay';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-powerplay-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_powerplay_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_powerplay_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_powerplay';

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
		? 'full_skater_powerplay.csv'
		: 'filtered_skater_powerplay.csv';

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
    register_rest_route('tsa/v1', '/skater-puckpossessions', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_puckpossessions',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_puckpossessions($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_puckpossessions';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'timeOnIcePerGame5v5',
		'satPct',
		'usatPct',
		'goalsPct',
		'individualSatForPer60',
		'individualShotsForPer60',
		'onIceShootingPct',
		'zoneStartPct',
		'offensiveZoneStartRatio',
		'offensiveZoneStartPct',
		'neutralZoneStartPct',
		'defensiveZoneStartPct',
		'faceoffPct5v5',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-puckpossessions-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_puckpossessions_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_puckpossessions_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_puckpossessions';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-puckpossessions-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_puckpossessions_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_puckpossessions_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_puckpossessions';

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
		? 'full_skater_puckpossessions.csv'
		: 'filtered_skater_puckpossessions.csv';

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
    register_rest_route('tsa/v1', '/skater-realtime', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_realtime',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_realtime($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_realtime';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'timeOnIcePerGame',
		'hits',
		'hitsPer60',
		'blockedShots',
		'blockedShotsPer60',
		'giveaways',
		'giveawaysPer60',
		'takeaways',
		'takeawaysPer60',
		'firstGoals',
		'otGoals',
		'emptyNetGoals',
		'emptyNetAssists',
		'emptyNetPoints',
		'totalShotAttempts',
		'shotAttemptsBlocked',
		'missedShots',
		'missedShotWideOfNet',
		'missedShotOverNet',
		'missedShotGoalpost',
		'missedShotCrossbar',
		'missedShotShort',
		'missedShotFailedBankAttempt',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-realtime-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_realtime_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_realtime_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_realtime';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-realtime-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_realtime_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_realtime_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_realtime';

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
		? 'full_skater_realtime.csv'
		: 'filtered_skater_realtime.csv';

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
    register_rest_route('tsa/v1', '/skater-scoringpergame', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_scoringpergame',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_scoringpergame($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_scoringpergame';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'goals',
		'assists',
		'totalPrimaryAssists',
		'totalSecondaryAssists',
		'points',
		'shots',
		'penaltyMinutes',
		'hits',
		'blockedShots',
		'timeOnIce',
		'goalsPerGame',
		'assistsPerGame',
		'primaryAssistsPerGame',
		'secondaryAssistsPerGame',
		'pointsPerGame',
		'shotsPerGame',
		'penaltyMinutesPerGame',
		'hitsPerGame',
		'blocksPerGame',
		'timeOnIcePerGame',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-scoringpergame-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_scoringpergame_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_scoringpergame_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_scoringpergame';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-scoringpergame-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_scoringpergame_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_scoringpergame_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_scoringpergame';

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
		? 'full_skater_scoringpergame.csv'
		: 'filtered_skater_scoringpergame.csv';

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
    register_rest_route('tsa/v1', '/skater-scoringrates', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_scoringrates',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_scoringrates($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_scoringrates';

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
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'timeOnIcePerGame5v5',
		'goals5v5',
		'assists5v5',
		'primaryAssists5v5',
		'secondaryAssists5v5',
		'points5v5',
		'goalsPer605v5',
		'assistsPer605v5',
		'primaryAssistsPer605v5',
		'secondaryAssistsPer605v5',
		'pointsPer605v5',
		'shootingPct5v5',
		'onIceShootingPct5v5',
		'offensiveZoneStartPct5v5',
		'satRelative5v5',
		'satPct',
		'netMinorPenaltiesPer60',
		'lastName'
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
    register_rest_route('tsa/v1', '/skater-scoringrates-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_scoringrates_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_scoringrates_meta($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_scoringrates';

    $min_date = $wpdb->get_var("SELECT MIN(gameDate) FROM $table");
    $max_date = $wpdb->get_var("SELECT MAX(gameDate) FROM $table");

    return [
        'min_date' => $min_date,
        'max_date' => $max_date,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-scoringrates-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_scoringrates_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_scoringrates_csv($request) {
    global $wpdb;
	
	$wpdb->query("SET NAMES utf8mb4");
	$wpdb->query("SET CHARACTER SET utf8mb4");

    $table = $wpdb->prefix . 'tsa_skater_scoringrates';

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
		? 'full_skater_scoringrates.csv'
		: 'filtered_skater_scoringrates.csv';

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