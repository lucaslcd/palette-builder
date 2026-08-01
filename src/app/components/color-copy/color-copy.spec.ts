import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ColorCopy } from './color-copy';

describe('ColorCopy', () => {
  let component: ColorCopy;
  let fixture: ComponentFixture<ColorCopy>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ColorCopy],
    }).compileComponents();

    fixture = TestBed.createComponent(ColorCopy);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
