var lccui = {

    action_setup: function () {

        // bind the quicksearch type select
        $('#qt').on('change', function(event) {

            if ($('#qt').val() == 'conesearch') {

                $('#q').attr(
                    'placeholder',
                    ('[RA] [Dec] '+
                     '[optional radius (arcmin)]')
                );
            }

            else if ($('#qt').val() == 'objectidsearch') {
                $('#q').attr(
                    'placeholder',
                    'e.g. EPIC216858738 / HAT-579-0111255 etc.'
                );

            }

            else if ($('#qt').val() == 'xmatchsearch') {
                $('#q').attr(
                    'placeholder',
                    'e.g. obj1 ra1 dec1, obj2 ra2 dec2, ...'
                );

            }

        });

    }

}
