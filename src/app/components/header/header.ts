import { Component, inject } from '@angular/core';
import { MatButton } from '@angular/material/button';
import { MatDialog } from '@angular/material/dialog';
import { MatToolbar } from "@angular/material/toolbar";
import { PaletteExportDialog } from '../palette-export-dialog/palette-export-dialog';
import { ColorState } from '../../services/color.state';
import { ExportService } from '../../services/export.service';
import { ExportState } from '../../services/export.state';

@Component({
  selector: 'pbu-header',
  imports: [MatToolbar, MatButton],
  templateUrl: './header.html',
  styleUrl: './header.css',
})
export class Header {
  colorState = inject(ColorState)
  exportState = inject(ExportState)

  dialog = inject(MatDialog)
  exportService = inject(ExportService)

  openExport() {
    const dialogRef = this.dialog.open(PaletteExportDialog, {
      data: { palette: this.colorState.state() },
    });
    dialogRef
      .afterClosed()
      .subscribe(({ success }) => {
        if(success) {
          this.exportService.export(this.colorState.state(), this.exportState.state().export)
        }
      })
  }
}
