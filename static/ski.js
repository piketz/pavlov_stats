function getRounds(Timestamp, Server_name) {

    var roundsRow = event.target.closest(".match-row").nextElementSibling;
        roundsRow.style.display = roundsRow.style.display === "none" ? "block" : "none";
    document.querySelectorAll('.rounds-row').forEach(function(row) {
    row.addEventListener('click', getRounds);
        });

    var roundsContainer = document.querySelector('.match-row[data-timestamp="' + Timestamp + '"] + .rounds-row .rounds');
    if (roundsContainer.innerHTML === '') {
        var xhr = new XMLHttpRequest();
        xhr.open('GET', '/rounds/' + Timestamp + '?Server_name=' + Server_name, true);
        xhr.onload = function() {

            if (this.status == 200) {
                var rounds = JSON.parse(this.responseText);
                var html = '<table>';
                html += '<thead><tr><th>Round</th><th>Round end time</th><th>Winning Team</th></tr></thead><tbody>';
                rounds.forEach(function(round) {
                    html += '<tr><td>' + round.Round + '</td><td>' + round.Timestamp + '</td><td>' + round.WinningTeam + '</td></tr>';
                });
                html += '</tbody></table>';
                roundsContainer.innerHTML = html;
            }
        };
        xhr.send();
    } else {
        roundsContainer.innerHTML = '';
    }
}


//document.querySelectorAll('.match-row').forEach(function(row) {
//    row.addEventListener('click', function() {
//        var Timestamp = this.getAttribute('data-timestamp');
//        var Server_name = '{{ Server_name }}';
//        var roundsRow = this.nextElementSibling;
//        if (roundsRow.classList.contains('rounds-row')) {
//            if (roundsRow.style.display === 'none') {
//                roundsRow.style.display = 'table-row';
//                getRounds(Timestamp, Server_name);
//            } else {
//                roundsRow.style.display = 'none';
//            }
//        }
//    });
//});


function sortTableByDate() {
     var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
     table = document.getElementsByClassName("table_sort")[0];
     switching = true;
     dir = "asc";
     while (switching) {
         switching = false;
         rows = table.rows;
         for (i = 1; i < (rows.length - 1); i++) {
             shouldSwitch = false;
             x = rows[i].getElementsByTagName("td")[0];
             y = rows[i + 1].getElementsByTagName("td")[0];
             if (dir == "asc") {
                 if (x.innerHTML > y.innerHTML) {
                     shouldSwitch = true;
                     break;
                 }
             } else if (dir == "desc") {
                 if (x.innerHTML < y.innerHTML) {
                     shouldSwitch = true;
                     break;
                 }
             }
         }
         if (shouldSwitch) {
             rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
             switching = true;
             switchcount ++;
         } else {
             if (switchcount == 0 && dir == "asc") {
                 dir = "desc";
                 switching = true;
             }
         }
     }
}

function sortTable(n) {
  var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
  table = document.getElementsByClassName("table_sort")[0];
  switching = true;
  dir = "asc";
  while (switching) {
    switching = false;
    rows = table.rows;
    for (i = 1; i < (rows.length - 1); i++) {
      shouldSwitch = false;
      x = rows[i].getElementsByTagName("td")[n];
      y = rows[i + 1].getElementsByTagName("td")[n];
      if (dir == "asc") {
        if (isNaN(parseFloat(x.innerHTML))) {
          if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {
            shouldSwitch= true;
            break;
          }
        } else {
          if (parseFloat(x.innerHTML) > parseFloat(y.innerHTML)) {
            shouldSwitch= true;
            break;
          }
        }
      } else if (dir == "desc") {
        if (isNaN(parseFloat(x.innerHTML))) {
          if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {
            shouldSwitch= true;
            break;
          }
        } else {
          if (parseFloat(x.innerHTML) < parseFloat(y.innerHTML)) {
            shouldSwitch = true;
            break;
          }
        }
      }
    }
    if (shouldSwitch) {
      rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
      switching = true;
      switchcount ++;
    } else {
      if (switchcount == 0 && dir == "asc") {
        dir = "desc";
        switching = true;
      }
    }
  }
}