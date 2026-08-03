import { Component, computed, inject, input } from '@angular/core';
import { UtilsService } from '../../services/utils.service';
import { ClipboardModule } from '@angular/cdk/clipboard';
import { MatSnackBar } from '@angular/material/snack-bar';
import { MatTooltip } from '@angular/material/tooltip';

@Component({
  selector: 'pbu-color-copy',
  imports: [ClipboardModule, MatTooltip],
  templateUrl: './color-copy.html',
  styleUrl: './color-copy.css',
  host: {
    "[style.backgroundColor]":"color()"
  }
})
export class ColorCopy {
  utilsService = inject(UtilsService)
  snackBar = inject(MatSnackBar)

  hue = input.required<number>()
  saturation = input.required<number>()
  light = input.required<number>()

  color = computed(()=>this.utilsService.formatHsl(this.hue(), this.saturation(), this.light()))
  hexColor = computed(()=>{
    const rgb = this.utilsService.hslToRgb(this.hue(), this.saturation(), this.light())
    return this.utilsService.rgbToHex(rgb[0], rgb[1], rgb[2])
  })

  onCopied() {
    this.snackBar.open('Color copied to clipboard', 'Close', {
      duration: 5000
    });
  }
}
